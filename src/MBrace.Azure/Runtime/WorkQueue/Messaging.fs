namespace MBrace.Azure.Runtime

open System
open System.IO
open System.Threading.Tasks

open Microsoft.ServiceBus.Messaging

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.Retry

open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities

type internal MessagingClient =
    /// Generic work item lease token dequeue method
    static member TryDequeue (clusterId : ClusterId, logger : ISystemLogger, localWorkerId : IWorkerId, dequeueF : unit -> Task<BrokeredMessage>) : Async<ICloudWorkItemLeaseToken option> = async { 
        let! (message : BrokeredMessage) = dequeueF() |> Async.AwaitTaskCorrect
        if message = null then 
            return None
        else 
            let workInfo = WorkItemLeaseTokenInfo.FromReceivedMessage message
            logger.Logf LogLevel.Debug "%O : dequeued, delivery count = %d" workInfo workInfo.DeliveryCount 

            logger.Logf LogLevel.Debug "%O : starting lock renew loop" workInfo
            let monitor = WorkItemLeaseMonitor.Start(clusterId, message, workInfo, logger)

            try
                let newRecord = new WorkItemRecord(workInfo.ProcessId, fromGuid workInfo.WorkItemId)
                newRecord.ETag <- "*"
                newRecord.Completed <- nullable false
                newRecord.DequeueTime <- nullable workInfo.DequeueTime
                newRecord.Status <- nullable(int WorkItemStatus.Dequeued)
                newRecord.CurrentWorker <- localWorkerId.Id
                newRecord.DeliveryCount <- nullable workInfo.DeliveryCount
                newRecord.FaultInfo <- nullable(int FaultInfo.NoFault)

                // determine the fault info for the dequeued work item
                let! faultInfo = async {
                    let faultCount = workInfo.DeliveryCount - 1
                    match workInfo.TargetWorker with
                    | Some target when target <> localWorkerId.Id -> 
                        // a targeted work item that has been dequeued by a different worker is to be declared faulted
                        newRecord.FaultInfo <- nullable(int FaultInfo.IsTargetedWorkItemOfDeadWorker)
                        return IsTargetedWorkItemOfDeadWorker(faultCount, new WorkerId(target))

                    | _ when faultCount = 0 -> return NoFault
                    | _ ->
                        let! oldRecord = Table.read<WorkItemRecord> clusterId.StorageAccount clusterId.RuntimeTable workInfo.ProcessId (fromGuid workInfo.WorkItemId)
                        match enum<FaultInfo> oldRecord.FaultInfo.Value with
                        | FaultInfo.FaultDeclaredByWorker -> // a fault exception has been set by the executing worker
                            let lastExc = ProcessConfiguration.JsonSerializer.UnPickleOfString<ExceptionDispatchInfo>(oldRecord.LastException)
                            let lastWorker = new WorkerId(oldRecord.CurrentWorker)
                            return FaultDeclaredByWorker(faultCount, lastExc, lastWorker)
                        | _ -> // a worker has died while previously dequeueing the worker
                            newRecord.FaultInfo <- nullable(int FaultInfo.WorkerDeathWhileProcessingWorkItem)
                            // account for cases where worker died before even updating the work item record
                            let previousWorker = match oldRecord.CurrentWorker with null -> "<unknown>" | w -> w
                            return WorkerDeathWhileProcessingWorkItem(faultCount, new WorkerId(previousWorker))
                }

                logger.Logf LogLevel.Debug "%O : extracted fault info %A" workInfo faultInfo
                logger.Logf LogLevel.Debug "%O : changing status to %A" workInfo WorkItemStatus.Dequeued
                let! _record = Table.merge clusterId.StorageAccount clusterId.RuntimeTable newRecord
                logger.Logf LogLevel.Debug "%O : changed status successfully" workInfo
                let! leaseToken = WorkItemLeaseToken.Create(clusterId, workInfo, monitor, faultInfo)
                return Some (leaseToken :> ICloudWorkItemLeaseToken)

            with e ->
                monitor.CompleteWith Abandon // in case of dequeue exception, abandon lease renew loop
                return! Async.Raise e
    }

    /// Generic work item enqueue method
    static member Enqueue (clusterId : ClusterId, logger : ISystemLogger, workItem : CloudWorkItem, allowNewSifts : bool, sendF : BrokeredMessage -> Task) = async { 
        // Step 1: initial record entry creation
        let record = WorkItemRecord.FromCloudWorkItem(workItem)
        do! Table.insert clusterId.StorageAccount clusterId.RuntimeTable record
        logger.Logf LogLevel.Debug "workItem:%O : begin enqueue" workItem.Id

        // Step 2: Persist work item payload to blob store
        let blobUri = sprintf "workItem/%s/%s" workItem.Process.Id (fromGuid workItem.Id)
        do! BlobPersist.PersistClosure<MessagePayload>(clusterId, Single workItem, blobUri, allowNewSifts)
        let! size = BlobPersist.GetPersistedClosureSize(clusterId, blobUri)

        // Step 3: update record entry
        let newRecord = record.CloneDefault()
        newRecord.Status <- nullable(int WorkItemStatus.Enqueued)
        newRecord.EnqueueTime <- nullable record.Timestamp
        newRecord.Size <- nullable size
        newRecord.FaultInfo <- nullable(int FaultInfo.NoFault)
        newRecord.ETag <- "*"
        let! _record = Table.merge clusterId.StorageAccount clusterId.RuntimeTable newRecord

        // Step 4: send work item message to service bus queue
        let msg = new BrokeredMessage(blobUri)
        msg.Properties.[ServiceBusSettings.WorkItemIdProperty] <- workItem.Id
        msg.Properties.[ServiceBusSettings.ParentTaskIdProperty] <- workItem.Process.Id
        workItem.TargetWorker |> Option.iter (fun t -> msg.Properties.[ServiceBusSettings.AffinityProperty] <- t.Id)
        do! sendF msg |> Async.AwaitTaskCorrect

        logger.Logf LogLevel.Debug "workItem:%O : enqueue completed, size %s" workItem.Id (getHumanReadableByteSize size)
    }

    /// Generic work item batch enqueue method
    static member EnqueueBatch(clusterId : ClusterId, logger : ISystemLogger, jobs : CloudWorkItem [], sendF : BrokeredMessage seq -> Task) = async { 
        if jobs.Length = 0 then return () else // silent discard if empty
        // Step 1: initial work item record population
        let records = jobs |> Seq.map WorkItemRecord.FromCloudWorkItem
        do! Table.insertBatch clusterId.StorageAccount clusterId.RuntimeTable records

        // Step 2: persist payload to blob store
        let headJob = jobs.[0]
        let blobUri = sprintf "workItem/%s/batch/%s" headJob.Process.Id (fromGuid headJob.Id)
        do! BlobPersist.PersistClosure<MessagePayload>(clusterId, Batch jobs, blobUri, allowNewSifts = false)
        let! size = BlobPersist.GetPersistedClosureSize(clusterId, blobUri)

        // Step 3: update runtime records
        let now = DateTimeOffset.Now
        let newRecords = 
            records |> Seq.map (fun r -> 
                let newRec = r.CloneDefault()
                newRec.ETag <- "*"
                newRec.Status <- nullable(int WorkItemStatus.Enqueued)
                newRec.EnqueueTime <- nullable now
                newRec.FaultInfo <- nullable(int FaultInfo.NoFault)
                newRec.Size <- nullable(size)
                newRec)

        do! Table.mergeBatch clusterId.StorageAccount clusterId.RuntimeTable newRecords

        // Step 4: create work messages and post to service bus queue
        let mkWorkItemMessage (i : int) (workItem : CloudWorkItem) =
            let msg = new BrokeredMessage(blobUri)
            msg.Properties.[ServiceBusSettings.WorkItemIdProperty] <- workItem.Id
            msg.Properties.[ServiceBusSettings.ParentTaskIdProperty] <- workItem.Process.Id
            msg.Properties.[ServiceBusSettings.BatchIndexProperty] <- i
            workItem.TargetWorker |> Option.iter (fun t -> msg.Properties.[ServiceBusSettings.AffinityProperty] <- t.Id)
            msg

        let messages = jobs |> Array.mapi mkWorkItemMessage
        do! sendF messages |> Async.AwaitTaskCorrect
        logger.Logf LogLevel.Info "Enqueued batched jobs of %d items for task %s, total size %s." jobs.Length headJob.Process.Id (getHumanReadableByteSize size)
    }
    

/// Topic subscription client
[<Sealed; AutoSerializable(false)>]
type internal Subscription (clusterId : ClusterId, targetWorkerId : IWorkerId, logger : ISystemLogger) = 
    do 
        let nsClient = clusterId.ServiceBusAccount.NamespaceManager
        let topic = clusterId.WorkItemTopic
        let affinity = targetWorkerId.Id
        if not <| nsClient.SubscriptionExists(topic, affinity) then 
            logger.Logf LogLevel.Info "Creating new subscription for %A" affinity
            let sd = new SubscriptionDescription(topic, affinity)
            sd.DefaultMessageTimeToLive <- ServiceBusSettings.MaxTTL
            sd.LockDuration <- ServiceBusSettings.MaxLockDuration
            sd.AutoDeleteOnIdle <- ServiceBusSettings.SubscriptionAutoDeleteInterval
            let filter = new SqlFilter(sprintf "%s = '%s'" ServiceBusSettings.AffinityProperty affinity)
            let _description = 
                retry (RetryPolicy.ExponentialDelay(3, 1.<sec>)) 
                      (fun () -> nsClient.CreateSubscription(sd, filter))
            ()
            

    let subscription = clusterId.ServiceBusAccount.CreateSubscriptionClient(clusterId.WorkItemTopic, targetWorkerId.Id)

    member this.TargetWorkerId = targetWorkerId

    member this.GetMessageCountAsync() = async {
        let! (descr : SubscriptionDescription) = clusterId.ServiceBusAccount.NamespaceManager.GetSubscriptionAsync(clusterId.WorkItemTopic, targetWorkerId.Id) |> Async.AwaitTaskCorrect
        return descr.MessageCount
    }

    member this.TryDequeue(currentWorker : IWorkerId) : Async<ICloudWorkItemLeaseToken option> = 
        MessagingClient.TryDequeue(clusterId, logger, currentWorker, fun () -> subscription.ReceiveAsync(ServiceBusSettings.ServerWaitTime))

    member this.DequeueAllMessagesBatch() = async { 
        let! mc = this.GetMessageCountAsync()
        if mc < 1L then return [||]
        else
            let! messages = subscription.ReceiveBatchAsync(int mc) |> Async.AwaitTaskCorrect
            return Seq.toArray messages
    }

/// Topic client implementation
[<Sealed; AutoSerializable(false)>]
type internal Topic (clusterId : ClusterId, logger : ISystemLogger) = 
    let topic = clusterId.ServiceBusAccount.CreateTopicClient(clusterId.WorkItemTopic)

    member this.GetMessageCountAsync() = async {
        let! (td : TopicDescription) = clusterId.ServiceBusAccount.NamespaceManager.GetTopicAsync(clusterId.WorkItemTopic) |> Async.AwaitTaskCorrect
        return td.MessageCountDetails.ActiveMessageCount
    }

    member this.GetSubscription(subscriptionId : IWorkerId) : Subscription = new Subscription(clusterId, subscriptionId, logger)
    
    member this.EnqueueBatch(jobs : CloudWorkItem []) : Async<unit> = 
        MessagingClient.EnqueueBatch(clusterId, logger, jobs, topic.SendBatchAsync)
    
    member this.Enqueue(workItem : CloudWorkItem, allowNewSifts : bool) = 
        MessagingClient.Enqueue(clusterId, logger, workItem, allowNewSifts, topic.SendAsync)

    static member Create(config, logger : ISystemLogger) = async { 
        let! exists = config.ServiceBusAccount.NamespaceManager.TopicExistsAsync(config.WorkItemTopic) |> Async.AwaitTaskCorrect
        if not exists then 
            logger.Logf LogLevel.Info "Creating new ServiceBus topic %A" config.WorkItemTopic
            let metadata = Metadata.Create config
            let qd = new TopicDescription(config.WorkItemTopic)
            qd.EnableBatchedOperations <- true
            qd.EnablePartitioning <- true
            qd.DefaultMessageTimeToLive <- ServiceBusSettings.MaxTTL
            qd.UserMetadata <- Metadata.ToJson metadata
            do! Queue.createQueueSafe (config.ServiceBusAccount.NamespaceManager.CreateTopicAsync(qd) |> Async.AwaitTaskCorrect |> Async.Ignore)
        else
            logger.Logf LogLevel.Info "Topic %A already exists." config.WorkItemTopic
        return new Topic(config, logger)
    }

/// Queue client implementation
[<Sealed; AutoSerializable(false)>]
type internal Queue (clusterId : ClusterId, logger : ISystemLogger) = 
    let queue = clusterId.ServiceBusAccount.CreateQueueClient(clusterId.WorkItemQueue, ReceiveMode.PeekLock)

    member this.GetMessageCountAsync() = async {
        let! (qd : QueueDescription) = clusterId.ServiceBusAccount.NamespaceManager.GetQueueAsync(clusterId.WorkItemQueue) |> Async.AwaitTaskCorrect
        return qd.MessageCount
    }

    member this.EnqueueBatch(jobs : CloudWorkItem []) = 
        MessagingClient.EnqueueBatch(clusterId, logger, jobs, queue.SendBatchAsync)
    
    member this.Enqueue(workItem : CloudWorkItem, allowNewSifts : bool) = 
        MessagingClient.Enqueue(clusterId, logger, workItem, allowNewSifts, queue.SendAsync)
    
    member this.TryDequeue(workerId : IWorkerId) : Async<ICloudWorkItemLeaseToken option> = 
        MessagingClient.TryDequeue(clusterId, logger, workerId, fun () -> queue.ReceiveAsync(ServiceBusSettings.ServerWaitTime))

    member this.EnqueueMessagesBatch(messages : seq<BrokeredMessage>) = async { return! queue.SendBatchAsync messages |> Async.AwaitTaskCorrect }
        
    static member Create(clusterId : ClusterId, logger : ISystemLogger) = async { 
        let ns = clusterId.ServiceBusAccount.NamespaceManager
        let! exists = ns.QueueExistsAsync(clusterId.WorkItemQueue) |> Async.AwaitTaskCorrect
        if not exists then 
            logger.Logf LogLevel.Info "Creating new ServiceBus queue %A" clusterId.WorkItemQueue
            let metadata = Metadata.Create clusterId
            let qd = new QueueDescription(clusterId.WorkItemQueue)
            qd.EnableBatchedOperations <- true
            qd.EnablePartitioning <- true
            qd.DefaultMessageTimeToLive <- ServiceBusSettings.MaxTTL 
            qd.MaxDeliveryCount <- ServiceBusSettings.MaxDeliveryCount
            qd.LockDuration <- ServiceBusSettings.MaxLockDuration
            qd.UserMetadata <- Metadata.ToJson metadata
            do! Queue.createQueueSafe (ns.CreateQueueAsync(qd) |> Async.AwaitTaskCorrect |> Async.Ignore)
        else
            logger.Logf LogLevel.Info "Queue %A already exists." clusterId.WorkItemQueue
        return new Queue(clusterId, logger)
    }