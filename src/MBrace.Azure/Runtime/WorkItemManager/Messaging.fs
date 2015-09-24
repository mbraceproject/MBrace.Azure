namespace MBrace.Azure.Runtime

open System
open System.IO
open System.Threading.Tasks

open Microsoft.ServiceBus.Messaging

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.Retry
open MBrace.Runtime.Components
open System.Runtime.Serialization

open MBrace.Azure.Runtime.Utilities

/// Common settings for queue, topic and messages.
type internal Settings private () = 
    /// Max number of deliveries before deadlettering.
    static member MaxDeliveryCount = Int32.MaxValue
    /// Message lock renew interval (in milliseconds).
    static member RenewLockInverval = 10000 
    /// Maximum message lock duration (5 minutes is the max value).
    static member MaxLockDuration = TimeSpan.FromSeconds(60.)
    /// Maximum message TTL.
    static member MaxTTL = TimeSpan.MaxValue
    /// Server wait time for dequeue.
    static member ServerWaitTime = TimeSpan.FromMilliseconds(50.)
    /// Affinity.
    static member AffinityProperty = "worker"
    /// ParentTaskId.
    static member ParentTaskIdProperty = "parentId"
    /// BatchIndex.
    static member BatchIndexProperty = "batchIndex"
    /// WorkItemId.
    static member WorkItemIdProperty = "uuid"
    /// SUbscription queue auto delete interval.
    static member SubscriptionAutoDeleteInterval = TimeSpan.MaxValue 

/// Info stored in BrokeredMessage.
type internal WorkItemLeaseTokenInfo =
    {
        ConfigurationId : ConfigurationId
        WorkItemId : Guid
        ContentId : string
        MessageLockId : Guid
        ParentWorkItemId : string
        BatchIndex : int option
        TargetWorker : string option
        DeliveryCount : int
        DequeueTime : DateTimeOffset
    }

    override this.ToString() = sprintf "leaseinfo:%A" this.WorkItemId

[<Sealed; AbstractClass>]
type internal WorkItemLeaseMonitor private () = 
    
    static member Start(message : BrokeredMessage, token : WorkItemLeaseTokenInfo, logger : ISystemLogger) = 
        Async.Start(
            let rec renewLoop() = 
                async { 
                    // NOTE : WorkItemLeaseMonitor Complete/Abandon should
                    // cause RenewLock to raise a MessageLostException, but this doesn't happen.
                    // As a workaround we stop the renewLoop when the table storage record .Complete is true.
                    let! tryRenew = 
                        async {
                            do! message.RenewLockAsync()
                            let now = DateTimeOffset.Now
                            let! record = Table.read<WorkItemRecord> token.ConfigurationId token.ConfigurationId.RuntimeTable token.ParentWorkItemId (fromGuid token.WorkItemId)
                            match record.Completed, record.Status with
                            | Nullable true, Nullable status when status = int WorkItemStatus.Completed || status = int WorkItemStatus.Faulted ->
                                return true
                            | _ ->
                                let updated = record.CloneDefault()
                                updated.ETag <- "*"
                                updated.RenewLockTime <- nullable now
                                let! _updated = Table.merge token.ConfigurationId token.ConfigurationId.RuntimeTable updated
                                return false
                        } |> Async.Catch 

                    match tryRenew with
                    | Choice1Of2 false ->
                        logger.Logf LogLevel.Debug "%A : lock renewed" token
                        do! Async.Sleep Settings.RenewLockInverval
                        return! renewLoop()
                    | Choice1Of2 true ->
                        logger.Logf LogLevel.Warning "%A : lock lost" token
                        message.Dispose()
                    | Choice2Of2 ex when (ex :? MessageLockLostException) -> 
                        logger.Logf LogLevel.Warning "%A : lock lost" token
                        message.Dispose()
                    | Choice2Of2 ex ->
                        logger.LogErrorf "%A : lock renew failed with %A" token ex
                        do! Async.Sleep Settings.RenewLockInverval
                        return! renewLoop()                                    
                }
            renewLoop())
    
    static member Complete(token : WorkItemLeaseTokenInfo) : Async<unit> = 
        async { 
            let config = token.ConfigurationId
            match token.TargetWorker with
            | None -> 
                let queue = 
                    ConfigurationRegistry.Resolve<StoreClientProvider>(config)
                        .QueueClient(config.RuntimeQueue, ReceiveMode.PeekLock)
                return! queue.CompleteAsync(token.MessageLockId)
            | Some aff -> 
                let subscription = 
                    ConfigurationRegistry.Resolve<StoreClientProvider>(config)
                        .SubscriptionClient(config.RuntimeTopic, aff)
                return! subscription.CompleteAsync(token.MessageLockId)
        }

    static member Abandon(token : WorkItemLeaseTokenInfo) : Async<unit> = 
        async { 
            let config = token.ConfigurationId
            match token.TargetWorker with
            | None -> 
                let queue = 
                    ConfigurationRegistry.Resolve<StoreClientProvider>(config)
                        .QueueClient(config.RuntimeQueue, ReceiveMode.PeekLock)
                return! queue.AbandonAsync(token.MessageLockId)
            | Some aff -> 
                let subscription = 
                    ConfigurationRegistry.Resolve<StoreClientProvider>(config)
                        .SubscriptionClient(config.RuntimeTopic, aff)
                return! subscription.AbandonAsync(token.MessageLockId)
        }

[<AutoSerializable(true); DataContract>]
type WorkItemLeaseToken internal (info : WorkItemLeaseTokenInfo, faultInfo : CloudWorkItemFaultInfo)  = 
    let [<DataMember(Name="info")>] info = info
    let [<DataMember(Name="faultInfo")>] faultInfo = faultInfo
    let [<IgnoreDataMember>] mutable record : WorkItemRecord = null
        
    let init () =
        record <- Async.RunSync(Table.read info.ConfigurationId info.ConfigurationId.RuntimeTable info.ParentWorkItemId (fromGuid info.WorkItemId))

    [<OnDeserialized>]
    let _onDeserialized (_ : StreamingContext) = init ()

    do init ()

    member internal this.Info = info

    override this.ToString () = sprintf "lease:%A" info.WorkItemId

    interface ICloudWorkItemLeaseToken with
        member this.DeclareCompleted() : Async<unit> = 
            async {
                do! WorkItemLeaseMonitor.Complete(info)
                let record = new WorkItemRecord(info.ParentWorkItemId, fromGuid info.WorkItemId)
                record.Status <- nullable(int WorkItemStatus.Completed)
                record.CompletionTime <- nullable(DateTimeOffset.Now)
                record.Completed <- nullable true
                record.ETag <- "*" 
                let! _record = Table.merge info.ConfigurationId info.ConfigurationId.RuntimeTable record
                return ()
            }
        
        member this.DeclareFaulted(edi : ExceptionDispatchInfo) : Async<unit> = 
            async { 
                do! WorkItemLeaseMonitor.Abandon(info) // TODO : should this be Abandon or Complete?
                let record = new WorkItemRecord(info.ParentWorkItemId, fromGuid info.WorkItemId)
                record.Status <- nullable(int WorkItemStatus.Faulted)
                record.Completed <- nullable false
                record.CompletionTime <- nullableDefault
                record.LastException <- Config.Pickler.Pickle edi
                record.FaultInfo <- nullable(int FaultInfo.FaultDeclaredByWorker)
                record.ETag <- "*"
                let! _record = Table.merge info.ConfigurationId info.ConfigurationId.RuntimeTable record
                return () 
            }
        
        member this.FaultInfo : CloudWorkItemFaultInfo = faultInfo
        
        member this.GetWorkItem() : Async<CloudWorkItem> = 
            async { 
                let blob = Blob.FromPath(info.ConfigurationId, info.ParentWorkItemId, info.ContentId)
                use! stream = blob.OpenRead()
                match info.BatchIndex with
                | Some i -> 
                    let sifted = Config.Pickler.Deserialize<SiftedClosure<CloudWorkItem []>>(stream)
                    let! jobs = ClosureSifter.UnSiftClosure(info.ConfigurationId, sifted)
                    return jobs.[i]
                | _ ->
                    let sifted = Config.Pickler.Deserialize<SiftedClosure<CloudWorkItem>>(stream)
                    return! ClosureSifter.UnSiftClosure(info.ConfigurationId, sifted)
            }
        
        member this.Id : CloudWorkItemId = info.WorkItemId
        
        member this.WorkItemType : CloudWorkItemType =
            let jobKind = enum<WorkItemKind>(record.Kind.GetValueOrDefault(-1))
            match jobKind with
            | WorkItemKind.ProcessRoot -> ProcessRoot
            | WorkItemKind.Choice   -> ChoiceChild(record.Index.GetValueOrDefault(-1), record.MaxIndex.GetValueOrDefault(-1))
            | WorkItemKind.Parallel -> ParallelChild(record.Index.GetValueOrDefault(-1), record.MaxIndex.GetValueOrDefault(-1))
            | _ -> failwithf "Invalid WorkItemKind %d" <| int jobKind
        
        member this.Size : int64 = record.Size.GetValueOrDefault(-1L)
        
        member this.TargetWorker : IWorkerId option = 
            match info.TargetWorker with
            | None -> None
            | Some w -> Some(WorkerId(w) :> _)
        
        member this.Process : ICloudProcessEntry = CloudProcessEntry(info.ConfigurationId, info.ParentWorkItemId) :> _
        
        member this.Type : string = record.Type

[<Sealed; AbstractClass>]
type internal MessagingClient private () =
    static member TryDequeue (config : ConfigurationId, logger : ISystemLogger, localWorkerId : IWorkerId, dequeueF : unit -> Task<BrokeredMessage>) : Async<WorkItemLeaseToken option> =
        async { 
            let! (message : BrokeredMessage) = dequeueF()
            if message = null then 
                return None
            else 
                let tryGet name = 
                    match message.Properties.TryGetValue(name) with
                    | true, v -> Some(v :?> 'T)
                    | false, _ -> None
        
                let affinity = tryGet Settings.AffinityProperty
                let deliveryCount = message.DeliveryCount
                let parentId = fromGuid (message.Properties.[Settings.ParentTaskIdProperty] :?> Guid)
                let batchIndex = tryGet Settings.BatchIndexProperty
                let lockToken = message.LockToken
                let body = fromGuid (message.GetBody<Guid>())
                let jobId = message.Properties.[Settings.WorkItemIdProperty] :?> Guid
                let dequeueTime = DateTimeOffset.Now
                let jobInfo = {
                    WorkItemId = jobId
                    ContentId = body
                    ConfigurationId = config
                    MessageLockId = lockToken
                    ParentWorkItemId = parentId
                    BatchIndex = batchIndex
                    TargetWorker = affinity
                    DeliveryCount = deliveryCount
                    DequeueTime = dequeueTime
                }
                logger.Logf LogLevel.Debug "%O : dequeued, starting lock renew loop" jobInfo
                WorkItemLeaseMonitor.Start(message, jobInfo, logger)

                logger.Logf LogLevel.Debug "%O : changing status to %A" jobInfo WorkItemStatus.Dequeued
                let newRecord = new WorkItemRecord(jobInfo.ParentWorkItemId, fromGuid jobInfo.WorkItemId)
                newRecord.ETag <- "*"
                newRecord.Completed <- nullable false
                newRecord.DequeueTime <- nullable jobInfo.DequeueTime
                newRecord.Status <- nullable(int WorkItemStatus.Dequeued)
                newRecord.CurrentWorker <- localWorkerId.Id
                newRecord.DeliveryCount <- nullable jobInfo.DeliveryCount
                newRecord.FaultInfo <- nullable(int FaultInfo.NoFault)

                logger.Logf LogLevel.Debug "%O : fetching fault info" jobInfo
                let! faultInfo = async {
                    logger.Logf LogLevel.Debug "%O : delivery count = %d" jobInfo jobInfo.DeliveryCount
                    let faultCount = jobInfo.DeliveryCount - 1

                    if faultCount = 0 then
                        match jobInfo.TargetWorker with
                        | None -> return NoFault
                        | Some target when target = localWorkerId.Id -> return NoFault
                        | Some target ->
                            newRecord.FaultInfo <- nullable(int FaultInfo.IsTargetedWorkItemOfDeadWorker)
                            return IsTargetedWorkItemOfDeadWorker(faultCount, new WorkerId(target))
                    else
                        let! oldRecord = Table.read<WorkItemRecord> config config.RuntimeTable jobInfo.ParentWorkItemId (fromGuid jobInfo.WorkItemId)
                        // two cases:
                        match enum<FaultInfo> oldRecord.FaultInfo.Value with
                        // either worker declared workItem faulted
                        | FaultInfo.FaultDeclaredByWorker ->
                            let lastExc =
                                if oldRecord.LastException = null then Unchecked.defaultof<_>
                                else Config.Pickler.UnPickle<ExceptionDispatchInfo>(oldRecord.LastException)
                            let lastWorker = new WorkerId(oldRecord.CurrentWorker)
                            return FaultDeclaredByWorker(faultCount, lastExc, lastWorker)
                        // or worker died
                        | _ ->
                            match jobInfo.TargetWorker with
                            | None ->
                                return WorkerDeathWhileProcessingWorkItem(faultCount, new WorkerId(oldRecord.CurrentWorker))
                            | Some target when target = localWorkerId.Id ->
                                newRecord.FaultInfo <- nullable(int FaultInfo.WorkerDeathWhileProcessingWorkItem)
                                return WorkerDeathWhileProcessingWorkItem(faultCount, new WorkerId(oldRecord.CurrentWorker))
                            | Some target ->
                                newRecord.FaultInfo <- nullable(int FaultInfo.IsTargetedWorkItemOfDeadWorker)
                                return IsTargetedWorkItemOfDeadWorker(faultCount, new WorkerId(target))
                }

                logger.Logf LogLevel.Debug "%O : extracted fault info %A" jobInfo faultInfo
                let! _record = Table.merge config config.RuntimeTable newRecord
                logger.Logf LogLevel.Debug "%O : changed status successfully" jobInfo
                return Some(WorkItemLeaseToken(jobInfo, faultInfo))
        }

    static member Enqueue (config : ConfigurationId, logger : ISystemLogger, workItem : CloudWorkItem, allowNewSifts : bool, sendF : BrokeredMessage -> Task) =
        async { 
            logger.Logf LogLevel.Debug "workItem:%O : enqueue" workItem.Id
            let record = WorkItemRecord.FromCloudWorkItem(workItem)
            let! sift = ClosureSifter.SiftClosure(config, workItem, allowNewSifts)
            do! Table.insert config config.RuntimeTable record
            let! blob = Blob<SiftedClosure<CloudWorkItem>>.Create(config, workItem.Process.Id, fromGuid workItem.Id, fun () -> sift)
            let msg = new BrokeredMessage(workItem.Id)
            msg.Properties.Add(Settings.WorkItemIdProperty, workItem.Id)
            msg.Properties.Add(Settings.ParentTaskIdProperty, toGuid workItem.Process.Id)
            match workItem.TargetWorker with
            | Some target -> msg.Properties.Add(Settings.AffinityProperty, target.Id)
            | _ -> ()
            let newRecord = record.CloneDefault()
            newRecord.Status <- nullable(int WorkItemStatus.Enqueued)
            newRecord.EnqueueTime <- nullable record.Timestamp
            newRecord.Size <- nullable blob.Size
            newRecord.FaultInfo <- nullable(int FaultInfo.NoFault)
            newRecord.ETag <- "*"
            let! _record = Table.merge config config.RuntimeTable newRecord
            do! sendF msg
            logger.Logf LogLevel.Debug "workItem:%O : enqueue completed, size %s" workItem.Id (getHumanReadableByteSize blob.Size)
        }

    static member EnqueueBatch(config : ConfigurationId, logger : ISystemLogger, jobs : CloudWorkItem [], sendF : BrokeredMessage seq -> Task) =
        async { 
            if jobs.Length = 0 then return () else
            let taskId = jobs.[0].Process.Id // this is a valid assumption for all uses
            let records = jobs |> Seq.map WorkItemRecord.FromCloudWorkItem
            let! sifted = ClosureSifter.SiftClosure(config, jobs, allowNewSifts = false)
            do! Table.insertBatch config config.RuntimeTable records

            let blobName = guid()
            let! blob = Blob<SiftedClosure<CloudWorkItem []>>.Create(config, taskId, blobName, fun () -> sifted)
            let size = blob.Size

            let mkWorkItemMessage (i : int) (workItem : CloudWorkItem) =
                let msg = new BrokeredMessage(toGuid blobName)
                msg.Properties.Add(Settings.WorkItemIdProperty, workItem.Id)
                msg.Properties.Add(Settings.ParentTaskIdProperty, toGuid workItem.Process.Id)
                msg.Properties.Add(Settings.BatchIndexProperty, i)
                match workItem.TargetWorker with
                | Some target -> msg.Properties.Add(Settings.AffinityProperty, target.Id)
                | _ -> ()
                msg

            let messages = jobs |> Array.mapi mkWorkItemMessage
            

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

            do! Table.mergeBatch config config.RuntimeTable newRecords
            do! sendF messages
            logger.Logf LogLevel.Info "Enqueued batched jobs of %d items for task %s, total size %s." jobs.Length taskId (getHumanReadableByteSize size)
        }
    

/// Local queue subscription client
[<AutoSerializable(false)>]
type internal Subscription (config : ConfigurationId, localWorkerId : IWorkerId, targetWorkerId : IWorkerId, logger : ISystemLogger) = 
    let clientFactory = ConfigurationRegistry.Resolve<StoreClientProvider>(config)

    do 
        let nsClient = clientFactory.NamespaceClient
        let topic = config.RuntimeTopic
        let affinity = targetWorkerId.Id
        if not <| nsClient.SubscriptionExists(topic, affinity) then 
            logger.Logf LogLevel.Info "Creating new subscription for %A" affinity
            let sd = new SubscriptionDescription(topic, affinity)
            sd.DefaultMessageTimeToLive <- Settings.MaxTTL
            sd.LockDuration <- Settings.MaxLockDuration
            sd.AutoDeleteOnIdle <- Settings.SubscriptionAutoDeleteInterval
            let filter = new SqlFilter(sprintf "%s = '%s'" Settings.AffinityProperty affinity)
            let _description = 
                retry (RetryPolicy.ExponentialDelay(3, 1.<sec>)) 
                      (fun () -> nsClient.CreateSubscription(sd, filter))
            ()
            

    let subscription = clientFactory.SubscriptionClient(config.RuntimeTopic, targetWorkerId.Id)

    member this.TargetWorkerId = targetWorkerId

    member this.MessageCount = SubscriptionDescription(config.RuntimeTopic, targetWorkerId.Id).MessageCount

    member this.TryDequeue() : Async<WorkItemLeaseToken option> = 
        MessagingClient.TryDequeue(config, logger, localWorkerId, fun () -> subscription.ReceiveAsync(Settings.ServerWaitTime))
        

/// Local queue topic client
[<AutoSerializable(false)>]
type internal Topic (config : ConfigurationId, logger : ISystemLogger) = 
    let clientFactory = ConfigurationRegistry.Resolve<StoreClientProvider>(config)
    let topic = clientFactory.TopicClient(config.RuntimeTopic)
    
    member val LocalWorkerId = Unchecked.defaultof<_> with get, set

    member this.MessageCount = TopicDescription(config.RuntimeTopic).MessageCountDetails.ActiveMessageCount

    member this.GetSubscription(workerId : IWorkerId) : Subscription = new Subscription(config, this.LocalWorkerId, workerId, logger)
    
    member this.EnqueueBatch(jobs : CloudWorkItem []) : Async<unit> = 
        MessagingClient.EnqueueBatch(config, logger, jobs, topic.SendBatchAsync)
    
    member this.Enqueue(workItem : CloudWorkItem, allowNewSifts : bool) = 
        MessagingClient.Enqueue(config, logger, workItem, allowNewSifts, topic.SendAsync)

    static member Create(config, logger : ISystemLogger) = 
        async { 
            let ns = ConfigurationRegistry.Resolve<StoreClientProvider>(config).NamespaceClient
            let! exists = ns.TopicExistsAsync(config.RuntimeTopic)
            if not exists then 
                logger.Logf LogLevel.Info "Creating new topic %A" config.RuntimeTopic
                let qd = new TopicDescription(config.RuntimeTopic)
                qd.EnableBatchedOperations <- true
                qd.EnablePartitioning <- true
                qd.DefaultMessageTimeToLive <- Settings.MaxTTL
                qd.UserMetadata <- Metadata.toString ReleaseInfo.localVersion config
                do! ns.CreateTopicAsync(qd)
            else
                logger.Logf  LogLevel.Info "Topic %A exists." config.RuntimeTopic
            return new Topic(config, logger)
        }

/// Queue client implementation
[<AutoSerializable(false)>]
type internal Queue (config : ConfigurationId, logger : ISystemLogger) = 
    let queue = ConfigurationRegistry.Resolve<StoreClientProvider>(config).QueueClient(config.RuntimeQueue, ReceiveMode.PeekLock)

    member val LocalWorkerId = Unchecked.defaultof<_> with get, set

    member this.MessageCount = QueueDescription(config.RuntimeQueue).MessageCount

    member this.EnqueueBatch(jobs : CloudWorkItem []) = 
        MessagingClient.EnqueueBatch(config, logger, jobs, queue.SendBatchAsync)
    
    member this.Enqueue(workItem : CloudWorkItem, allowNewSifts : bool) = 
        MessagingClient.Enqueue(config, logger, workItem, allowNewSifts, queue.SendAsync)
    
    member this.TryDequeue() : Async<WorkItemLeaseToken option> = 
        MessagingClient.TryDequeue(config, logger, this.LocalWorkerId, fun () -> queue.ReceiveAsync(Settings.ServerWaitTime))
        
    static member Create(config : ConfigurationId, logger : ISystemLogger) = 
        async { 
            let ns = ConfigurationRegistry.Resolve<StoreClientProvider>(config).NamespaceClient
            let! exists = ns.QueueExistsAsync(config.RuntimeQueue)
            if not exists then 
                logger.Logf LogLevel.Info "Creating new queue %A" config.RuntimeQueue
                let qd = new QueueDescription(config.RuntimeQueue)
                qd.EnableBatchedOperations <- true
                qd.EnablePartitioning <- true
                qd.DefaultMessageTimeToLive <- Settings.MaxTTL 
                qd.MaxDeliveryCount <- Settings.MaxDeliveryCount
                qd.LockDuration <- Settings.MaxLockDuration
                qd.UserMetadata <- Metadata.toString ReleaseInfo.localVersion config
                do! ns.CreateQueueAsync(qd)
            else
                logger.Logf LogLevel.Info "Queue %A exists." config.RuntimeQueue
            return new Queue(config, logger)
        }