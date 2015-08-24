namespace MBrace.Azure.Runtime

open System
open Microsoft.ServiceBus.Messaging
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities
open MBrace.Azure
open MBrace.Core.Internals
open System.IO
open MBrace.Runtime
open MBrace.Runtime.Utils
open System.Runtime.Serialization
open System.Threading.Tasks
open MBrace.Runtime.Utils.Retry

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
    /// JobId.
    static member JobIdProperty = "uuid"
    /// SUbscription queue auto delete interval.
    static member SubscriptionAutoDeleteInterval = TimeSpan.MaxValue 

/// Info stored in BrokeredMessage.
type internal JobLeaseTokenInfo =
    {
        ConfigurationId : ConfigurationId
        JobId : Guid
        ContentId : string
        MessageLockId : Guid
        ParentJobId : string
        BatchIndex : int option
        TargetWorker : string option
        DeliveryCount : int
        DequeueTime : DateTimeOffset
    }

    override this.ToString() = sprintf "leaseinfo:%A" this.JobId

[<Sealed; AbstractClass>]
type internal JobLeaseMonitor private () = 
    
    static member Start(message : BrokeredMessage, token : JobLeaseTokenInfo, logger : ISystemLogger) = 
        Async.Start(
            let rec renewLoop() = 
                async { 
                    // NOTE : JobLeaseMonitor Complete/Abandon should
                    // cause RenewLock to raise a MessageLostException, but this doesn't happen.
                    // As a workaround we stop the renewLoop when the table storage record .Complete is true.
                    let! tryRenew = 
                        async {
                            do! message.RenewLockAsync()
                            let now = DateTimeOffset.Now
                            let! record = Table.read<JobRecord> token.ConfigurationId token.ConfigurationId.RuntimeTable token.ParentJobId (fromGuid token.JobId)
                            match record.Completed, record.Status with
                            | Nullable true, Nullable status when status = int JobStatus.Completed || status = int JobStatus.Faulted ->
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
    
    static member Complete(token : JobLeaseTokenInfo) : Async<unit> = 
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

    static member Abandon(token : JobLeaseTokenInfo) : Async<unit> = 
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
type JobLeaseToken internal (info : JobLeaseTokenInfo, faultInfo : CloudJobFaultInfo)  = 
    let [<DataMember(Name="info")>] info = info
    let [<DataMember(Name="faultInfo")>] faultInfo = faultInfo
    let [<IgnoreDataMember>] mutable record : JobRecord = null
        
    let init () =
        record <- Async.RunSync(Table.read info.ConfigurationId info.ConfigurationId.RuntimeTable info.ParentJobId (fromGuid info.JobId))

    [<OnDeserialized>]
    let _onDeserialized (_ : StreamingContext) = init ()

    do init ()

    member internal this.Info = info

    override this.ToString () = sprintf "lease:%A" info.JobId

    interface ICloudJobLeaseToken with
        member this.DeclareCompleted() : Async<unit> = 
            async {
                do! JobLeaseMonitor.Complete(info)
                let record = new JobRecord(info.ParentJobId, fromGuid info.JobId)
                record.Status <- nullable(int JobStatus.Completed)
                record.CompletionTime <- nullable(DateTimeOffset.Now)
                record.Completed <- nullable true
                record.ETag <- "*" 
                let! _record = Table.merge info.ConfigurationId info.ConfigurationId.RuntimeTable record
                return ()
            }
        
        member this.DeclareFaulted(edi : ExceptionDispatchInfo) : Async<unit> = 
            async { 
                do! JobLeaseMonitor.Abandon(info) // TODO : should this be Abandon or Complete?
                let record = new JobRecord(info.ParentJobId, fromGuid info.JobId)
                record.Status <- nullable(int JobStatus.Faulted)
                record.Completed <- nullable false
                record.CompletionTime <- nullableDefault
                record.LastException <- Config.Pickler.Pickle edi
                record.FaultInfo <- nullable(int FaultInfo.FaultDeclaredByWorker)
                record.ETag <- "*"
                let! _record = Table.merge info.ConfigurationId info.ConfigurationId.RuntimeTable record
                return () 
            }
        
        member this.FaultInfo : CloudJobFaultInfo = faultInfo
        
        member this.GetJob() : Async<CloudJob> = 
            async { 
                let blob = Blob.FromPath(info.ConfigurationId, info.ParentJobId, info.ContentId)
                use! stream = blob.OpenRead()
                match info.BatchIndex with
                | Some i -> 
                    let jobs = Config.Pickler.Deserialize<CloudJob []>(stream)
                    return jobs.[i]
                | _ ->
                    return Config.Pickler.Deserialize<CloudJob>(stream)
            }
        
        member this.Id : CloudJobId = info.JobId
        
        member this.JobType : CloudJobType =
            let jobKind = enum<JobKind>(record.Kind.GetValueOrDefault(-1))
            match jobKind with
            | JobKind.TaskRoot -> TaskRoot
            | JobKind.Choice   -> ChoiceChild(record.Index.GetValueOrDefault(-1), record.MaxIndex.GetValueOrDefault(-1))
            | JobKind.Parallel -> ParallelChild(record.Index.GetValueOrDefault(-1), record.MaxIndex.GetValueOrDefault(-1))
            | _ -> failwithf "Invalid JobKind %d" <| int jobKind
        
        member this.Size : int64 = record.Size.GetValueOrDefault(-1L)
        
        member this.TargetWorker : IWorkerId option = 
            match info.TargetWorker with
            | None -> None
            | Some w -> Some(WorkerId(w) :> _)
        
        member this.TaskEntry : ICloudTaskCompletionSource = TaskCompletionSource(info.ConfigurationId, info.ParentJobId) :> _
        
        member this.Type : string = record.Type

[<Sealed; AbstractClass>]
type internal MessagingClient private () =
    static member inline TryDequeue (config : ConfigurationId, logger : ISystemLogger, localWorkerId : IWorkerId, dequeueF : unit -> Task<BrokeredMessage>) : Async<JobLeaseToken option> =
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
                let jobId = message.Properties.[Settings.JobIdProperty] :?> Guid
                let dequeueTime = DateTimeOffset.Now
                let jobInfo = {
                    JobId = jobId
                    ContentId = body
                    ConfigurationId = config
                    MessageLockId = lockToken
                    ParentJobId = parentId
                    BatchIndex = batchIndex
                    TargetWorker = affinity
                    DeliveryCount = deliveryCount
                    DequeueTime = dequeueTime
                }
                logger.Logf LogLevel.Debug "%O : dequeued, starting lock renew loop" jobInfo
                JobLeaseMonitor.Start(message, jobInfo, logger)

                logger.Logf LogLevel.Debug "%O : changing status to %A" jobInfo JobStatus.Dequeued
                let newRecord = new JobRecord(jobInfo.ParentJobId, fromGuid jobInfo.JobId)
                newRecord.ETag <- "*"
                newRecord.Completed <- nullable false
                newRecord.DequeueTime <- nullable jobInfo.DequeueTime
                newRecord.Status <- nullable(int JobStatus.Dequeued)
                newRecord.CurrentWorker <- localWorkerId.Id
                newRecord.DeliveryCount <- nullable jobInfo.DeliveryCount
                newRecord.FaultInfo <- nullable(int FaultInfo.NoFault)

                logger.Logf LogLevel.Debug "%O : fetching fault info" jobInfo
                let! faultInfo = async {
                    logger.Logf LogLevel.Debug "%O : delivery count = %d" jobInfo jobInfo.DeliveryCount
                    let faultCount = jobInfo.DeliveryCount - 1
                    // On first delivery no fault
                    if faultCount = 0 then
                        return NoFault
                    else
                        let! oldRecord = Table.read<JobRecord> config config.RuntimeTable jobInfo.ParentJobId (fromGuid jobInfo.JobId)
                        // two cases:
                        match enum<FaultInfo> oldRecord.FaultInfo.Value with
                        // either worker declared job faulted
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
                                return WorkerDeathWhileProcessingJob(faultCount, new WorkerId(oldRecord.CurrentWorker))
                            | Some target when target = localWorkerId.Id ->
                                newRecord.FaultInfo <- nullable(int FaultInfo.WorkerDeathWhileProcessingJob)
                                return WorkerDeathWhileProcessingJob(faultCount, new WorkerId(oldRecord.CurrentWorker))
                            | Some target ->
                                newRecord.FaultInfo <- nullable(int FaultInfo.IsTargetedJobOfDeadWorker)
                                return IsTargetedJobOfDeadWorker(faultCount, new WorkerId(target))
                }

                logger.Logf LogLevel.Debug "%O : extracted fault info %A" jobInfo faultInfo
                let! _record = Table.merge config config.RuntimeTable newRecord
                logger.Logf LogLevel.Debug "%O : changed status successfully" jobInfo
                return Some(JobLeaseToken(jobInfo, faultInfo))
        }

    static member inline Enqueue (config : ConfigurationId, logger : ISystemLogger, job : CloudJob, sendF : BrokeredMessage -> Task) =
        async { 
            logger.Logf LogLevel.Debug "job:%O : enqueue" job.Id
            let record = JobRecord.FromCloudJob(job)
            do! Table.insert config config.RuntimeTable record
            let! blob = Blob.Create(config, job.TaskEntry.Id, fromGuid job.Id, fun () -> job)
            let msg = new BrokeredMessage(job.Id)
            msg.Properties.Add(Settings.JobIdProperty, job.Id)
            msg.Properties.Add(Settings.ParentTaskIdProperty, toGuid job.TaskEntry.Id)
            match job.TargetWorker with
            | Some target -> msg.Properties.Add(Settings.AffinityProperty, target.Id)
            | _ -> ()
            do! sendF msg
            let newRecord = record.CloneDefault()
            newRecord.Status <- nullable(int JobStatus.Enqueued)
            newRecord.EnqueueTime <- nullable record.Timestamp
            newRecord.Size <- nullable blob.Size
            newRecord.FaultInfo <- nullable(int FaultInfo.NoFault)
            newRecord.ETag <- "*"
            let! _record = Table.merge config config.RuntimeTable newRecord
            logger.Logf LogLevel.Debug "job:%O : enqueue completed, size %s" job.Id (getHumanReadableByteSize blob.Size)
        }

    static member EnqueueBatch(config : ConfigurationId, logger : ISystemLogger, jobs : CloudJob [], sendF : BrokeredMessage seq -> Task) =
        async { 
            if jobs.Length = 0 then return () else
            let taskId = jobs.[0].TaskEntry.Id // this is a valid assumption for all uses
            let records = jobs |> Seq.map JobRecord.FromCloudJob
            do! Table.insertBatch config config.RuntimeTable records

            let blobName = guid()
            let! blob = Blob.Create(config, taskId, blobName, fun () -> jobs)
            let size = blob.Size

            let mkJobMessage (i : int) (job : CloudJob) =
                let msg = new BrokeredMessage(toGuid blobName)
                msg.Properties.Add(Settings.JobIdProperty, job.Id)
                msg.Properties.Add(Settings.ParentTaskIdProperty, toGuid job.TaskEntry.Id)
                msg.Properties.Add(Settings.BatchIndexProperty, i)
                match job.TargetWorker with
                | Some target -> msg.Properties.Add(Settings.AffinityProperty, target.Id)
                | _ -> ()
                msg

            let messages = jobs |> Array.mapi mkJobMessage
            do! sendF messages

            let now = DateTimeOffset.Now
            let newRecords = 
                records |> Seq.map (fun r -> 
                    let newRec = r.CloneDefault()
                    newRec.ETag <- "*"
                    newRec.Status <- nullable(int JobStatus.Enqueued)
                    newRec.EnqueueTime <- nullable now
                    newRec.FaultInfo <- nullable(int FaultInfo.NoFault)
                    newRec.Size <- nullable(size)
                    newRec)

            return! Table.mergeBatch config config.RuntimeTable newRecords
            logger.Logf LogLevel.Info "Enqueued batched jobs of %d items for task %s, total size %s." jobs.Length taskId (getHumanReadableByteSize size)
        }
    

/// Local queue subscription client
[<AutoSerializable(false)>]
type internal Subscription (config : ConfigurationId, localWorkerId : IWorkerId, targetWorkerId : IWorkerId, logger : ISystemLogger) = 
    let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config)

    do 
        let ns = cp.NamespaceClient
        let topic = config.RuntimeTopic
        let affinity = targetWorkerId.Id
        if not <| ns.SubscriptionExists(topic, affinity) then 
            logger.Logf LogLevel.Info "Creating new subscription for %A" affinity
            let sd = new SubscriptionDescription(topic, affinity)
            sd.DefaultMessageTimeToLive <- Settings.MaxTTL
            sd.LockDuration <- Settings.MaxLockDuration
            sd.AutoDeleteOnIdle <- Settings.SubscriptionAutoDeleteInterval
            let filter = new SqlFilter(sprintf "%s = '%s'" Settings.AffinityProperty affinity)
            let _description = 
                retry (RetryPolicy.ExponentialDelay(3, 1.<sec>)) 
                      (fun () -> ns.CreateSubscription(sd, filter))
            ()
            

    let sub = cp.SubscriptionClient(config.RuntimeTopic, targetWorkerId.Id)

    member this.TargetWorkerId = targetWorkerId

    member this.TryDequeue() : Async<JobLeaseToken option> = 
        MessagingClient.TryDequeue(config, logger, localWorkerId, fun () -> sub.ReceiveAsync(Settings.ServerWaitTime))
        

/// Local queue topic client
[<AutoSerializable(false)>]
type internal Topic (config : ConfigurationId, logger : ISystemLogger) = 
    let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config)
    let topic = cp.TopicClient(config.RuntimeTopic)
    
    member val LocalWorkerId = Unchecked.defaultof<_> with get, set

    member this.GetSubscription(workerId : IWorkerId) : Subscription = new Subscription(config, this.LocalWorkerId, workerId, logger)
    
    member this.EnqueueBatch(jobs : CloudJob []) : Async<unit> = 
        MessagingClient.EnqueueBatch(config, logger, jobs, topic.SendBatchAsync)
    
    member this.Enqueue(job : CloudJob) = 
        MessagingClient.Enqueue(config, logger, job, topic.SendAsync)

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

    member this.EnqueueBatch(jobs : CloudJob []) = 
        MessagingClient.EnqueueBatch(config, logger, jobs, queue.SendBatchAsync)
    
    member this.Enqueue(job : CloudJob) = 
        MessagingClient.Enqueue(config, logger, job, queue.SendAsync)
    
    member this.TryDequeue() : Async<JobLeaseToken option> = 
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