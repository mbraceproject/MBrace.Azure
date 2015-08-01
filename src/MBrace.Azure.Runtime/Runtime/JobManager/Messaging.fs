namespace MBrace.Azure.Runtime

open System
open Microsoft.ServiceBus.Messaging
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities
open MBrace.Azure
open MBrace.Core.Internals
open System.IO
open MBrace.Runtime
open System.Runtime.Serialization
open System.Threading.Tasks

type internal Settings private () = 
    static member RenewLockInverval = 10000 // message renew interval in ms
    static member MaxLockDuration = WorkerManager.MaxHeartbeatTimespan // 5 minutes, max value
    static member MaxTTL = TimeSpan.MaxValue
    static member ServerWaitTime = TimeSpan.FromMilliseconds(50.)
    static member AffinityProperty = "worker"
    static member ParentTaskIdProperty = "parentId"
    static member StreamOffsetProperty = "offset"
    static member JobIdProperty = "uuid"
    static member SubscriptionAutoDeleteInterval = TimeSpan.MaxValue 

/// Info stored in BrokeredMessage.
type internal JobLeaseTokenInfo =
    {
        ConfigurationId : ConfigurationId
        JobId : string
        ContentId : string
        MessageLockId : Guid
        ParentJobId : string
        Offset : int64 option 
        TargetWorker : string option
        DeliveryCount : int
        DequeueTime : DateTimeOffset
    }

    override this.ToString() = sprintf "leaseinfo:%A" this.JobId

[<Sealed; AbstractClass>]
type internal JobLeaseMonitor private () = 
    
    static member Start(message : BrokeredMessage, logger : ISystemLogger) = 
        Async.Start(let rec renewLoop() = 
                             async { 
                                 let! tryRenew = message.RenewLockAsync()
                                                 |> Async.AwaitTask
                                                 |> Async.Catch
                                 match tryRenew with
                                 | Choice1Of2 () ->
                                    do! Async.Sleep Settings.RenewLockInverval
                                    return! renewLoop()
                                 | Choice2Of2 ex when (ex :? MessageLockLostException) -> 
                                    logger.Logf LogLevel.Warning "Lock lost for message %A" <| message.GetBody<string>()
                                    return ()   
                                 | Choice2Of2 ex ->
                                    logger.LogErrorf "Lock loop for message %A failed with %A" <| message.GetBody<string>() <| ex
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
type JobLeaseToken internal (info : JobLeaseTokenInfo)  = 
    let [<DataMember(Name="info")>] info = info

    let [<IgnoreDataMember>] mutable record : JobRecord = null
        
    let init () =
        record <- Async.RunSync(Table.read info.ConfigurationId info.ConfigurationId.RuntimeTable info.ParentJobId info.JobId)

    [<OnDeserialized>]
    let _onDeserialized (_ : StreamingContext) = init ()

    do init ()

    member internal this.Info = info

    override this.ToString () = sprintf "lease:%A" info.JobId

    interface ICloudJobLeaseToken with
        member this.DeclareCompleted() : Async<unit> = 
            async {
                do! JobLeaseMonitor.Complete(info)
                let record = new JobRecord(info.ParentJobId, info.JobId)
                record.Status <- nullable(int JobStatus.Completed)
                record.CompletionTime <- nullable(DateTimeOffset.Now)
                record.ETag <- "*"
                let! _record = Table.merge info.ConfigurationId info.ConfigurationId.RuntimeTable record
                return ()
            }
        
        member this.DeclareFaulted(edi : ExceptionDispatchInfo) : Async<unit> = 
            async { 
                do! JobLeaseMonitor.Abandon(info) // TODO : should this be Abandon or Complete?
                let record = new JobRecord(info.ParentJobId, info.JobId)
                record.Status <- nullable(int JobStatus.Faulted)
                record.CompletionTime <- nullable(DateTimeOffset.Now)
                record.LastException <- Config.Pickler.Pickle edi
                record.FaultInfo <- nullable(int FaultInfo.FaultDeclaredByWorker)
                record.ETag <- "*"
                let! _record = Table.merge info.ConfigurationId info.ConfigurationId.RuntimeTable record
                return () 
            }
        
        member this.FaultInfo : JobFaultInfo = 
            // TODO : implement
            NoFault
        
        member this.GetJob() : Async<CloudJob> = 
            async { 
                let blob = Blob.FromPath(info.ConfigurationId, info.ParentJobId, info.ContentId)
                use! stream = blob.OpenRead()
                let _pos = 
                    match info.Offset with
                    | Some pos -> stream.Seek(pos, SeekOrigin.Begin)
                    | _ -> 0L
                return Config.Pickler.Deserialize<CloudJob>(stream)
            }
        
        member this.Id : string = info.JobId
        
        member this.JobType : JobType =
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
                let streamPos = tryGet Settings.StreamOffsetProperty
                let lockToken = message.LockToken
                let body = fromGuid (message.GetBody<Guid>())
                let jobId = fromGuid (message.Properties.[Settings.JobIdProperty] :?> Guid)
                let dequeueTime = DateTimeOffset.Now
                let info = {
                    JobId = jobId
                    ContentId = body
                    ConfigurationId = config
                    MessageLockId = lockToken
                    ParentJobId = parentId
                    Offset = streamPos
                    TargetWorker = affinity
                    DeliveryCount = deliveryCount
                    DequeueTime = dequeueTime
                }
                logger.Logf LogLevel.Debug "%O : dequeued, starting lock renew loop" info
                JobLeaseMonitor.Start(message, logger)

                logger.Logf LogLevel.Debug "%O : changing status to Dequeued" info
                let record = new JobRecord(info.ParentJobId, info.JobId)
                record.DequeueTime <- nullable info.DequeueTime
                record.Status <- nullable(int JobStatus.Dequeued)
                record.CurrentWorker <- localWorkerId.Id
                record.DeliveryCount <- nullable info.DeliveryCount

//                if info.DeliveryCount > 0 then
//                    record.FaultInfo <- nullable(int FaultInfo.WorkerDeathWhileProcessingJob)
//                    //FaultInfo.
                record.ETag <- "*"
                let! _record = Table.merge config config.RuntimeTable record
                logger.Logf LogLevel.Debug "%O : changed status successfully" info
                return Some(JobLeaseToken(info))
        }

    static member inline Enqueue (config : ConfigurationId, logger : ISystemLogger, job : CloudJob, sendF : BrokeredMessage -> Task) =
        async { 
            let! blob = Blob.Create(config, job.TaskEntry.Id, job.Id, fun () -> job)
            let msg = new BrokeredMessage(toGuid job.Id)
            msg.Properties.Add(Settings.JobIdProperty, toGuid job.Id)
            msg.Properties.Add(Settings.ParentTaskIdProperty, toGuid job.TaskEntry.Id)
            match job.TargetWorker with
            | Some target -> msg.Properties.Add(Settings.AffinityProperty, target.Id)
            | _ -> ()
            do! sendF msg
            return blob.Size
        }

    static member inline EnqueueBatch(config : ConfigurationId, logger : ISystemLogger, jobs : CloudJob [], sendF : BrokeredMessage seq -> Task) =
        async { 
            let ys = new ResizeArray<BrokeredMessage>(jobs.Length)
            let sizes = new ResizeArray<int64>(jobs.Length)
            for parentId, jobs in Seq.groupBy (fun j -> j.TaskEntry.Id) jobs do
                logger.Logf LogLevel.Info "Creating common job file for %d jobs, parent id = %O" (Seq.length jobs) parentId
                let temp = Path.GetTempFileName()
                let fileStream = File.OpenWrite(temp)
                let blobName = guid()
                let lastPosition = ref 0L
                for job in jobs do
                    Config.Pickler.Serialize(fileStream, job, leaveOpen = true)
                    let msg = new BrokeredMessage(toGuid blobName)
                    msg.Properties.Add(Settings.JobIdProperty, toGuid job.Id)
                    msg.Properties.Add(Settings.ParentTaskIdProperty, toGuid job.TaskEntry.Id)
                    msg.Properties.Add(Settings.StreamOffsetProperty, lastPosition.Value)
                    match job.TargetWorker with
                    | Some target -> msg.Properties.Add(Settings.AffinityProperty, target.Id)
                    | _ -> ()
                    ys.Add(msg)
                    sizes.Add(fileStream.Position - lastPosition.Value)
                    lastPosition := fileStream.Position
                fileStream.Flush()
                fileStream.Dispose()

                logger.Logf LogLevel.Info "Uploading job file for parent id = %O, %s." parentId (getHumanReadableByteSize lastPosition.Value)
                do! Blob.UploadFromFile(config, parentId, blobName, temp)
                    |> Async.Ignore
                File.Delete(temp)

            do! sendF ys
            return sizes :> seq<int64>
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
            ns.CreateSubscription(sd, filter) |> ignore

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
    
    member this.EnqueueBatch(jobs : CloudJob []) : Async<int64 seq> = 
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
                qd.LockDuration <- Settings.MaxLockDuration
                qd.UserMetadata <- Metadata.toString ReleaseInfo.localVersion config
                do! ns.CreateQueueAsync(qd)
            else
                logger.Logf LogLevel.Info "Queue %A exists." config.RuntimeQueue
            return new Queue(config, logger)
        }