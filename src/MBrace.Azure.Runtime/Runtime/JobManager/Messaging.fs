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

[<AutoOpenAttribute>]
module private Helpers = 
    let RenewLockInverval = 10000 // 10 minutes, same with heartbeat
    let MaxLockDuration = TimeSpan.FromMinutes(5.) // 5 minutes, max value
    let MaxTTL = TimeSpan.MaxValue
    let ServerWaitTime = TimeSpan.FromMilliseconds(50.)
    let AffinityPropertyName = "Affinity"
    let ParentTaskIdPropertyName = "ParentId"
    let StreamOffsetPropertyName = "StreamOffset"
    let SubscriptionAutoDeleteInterval = TimeSpan.MaxValue 

/// Info stored in BrokeredMessage.
type internal JobLeaseTokenInfo =
    {
        ConfigurationId : ConfigurationId
        JobId : string
        MessageLockId : Guid
        ParentJobId : string
        Offset : int64 option 
        TargetWorker : string option
        DeliveryCount : int
        DequeueTime : DateTimeOffset
    }

[<Sealed; AbstractClass>]
type internal JobLeaseMonitor private () = 
    
    static member Start(message : BrokeredMessage) = 
        Async.StartChild(let rec renewLoop() = 
                             async { 
                                 try 
                                     do! message.RenewLockAsync()
                                 with
                                 | :? MessageLockLostException -> return ()
                                 | _ -> ()
                                 do! Async.Sleep RenewLockInverval
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

[<AutoSerializable(true); DataContract>]
type JobLeaseToken private (info : JobLeaseTokenInfo)  = 
    let [<DataMember(Name="info")>] info = info

    let [<DataMember(Name="record")>] record : JobRecord = 
        Async.RunSync(Table.read info.ConfigurationId info.ConfigurationId.RuntimeTable info.ParentJobId info.JobId)

    member this.Id              = info.JobId
    member this.ConfigurationId = info.ConfigurationId
    member this.TargetWorker    = info.TargetWorker
    member this.MessageLockId   = info.MessageLockId
    member this.DeliveryCount   = info.DeliveryCount
    member this.ParentJobId     = info.ParentJobId
    member this.DequeueTime     = info.DequeueTime

    interface ICloudJobLeaseToken with
        member this.DeclareCompleted() : Async<unit> = 
            async {
                do! JobLeaseMonitor.Complete(info)
                let record = new JobRecord(info.ParentJobId, info.JobId)
                record.Status <- nullable(int JobStatus.Completed)
                record.CompletionTime <- nullable(DateTimeOffset.Now)
                let! _record = Table.merge info.ConfigurationId info.ConfigurationId.RuntimeTable record
                return ()
            }
        
        member this.DeclareFaulted(arg1 : ExceptionDispatchInfo) : Async<unit> = 
            async { 
                do! JobLeaseMonitor.Complete(info)
                let record = new JobRecord(info.ParentJobId, info.JobId)
                record.Status <- nullable(int JobStatus.Faulted)
                record.CompletionTime <- nullable(DateTimeOffset.Now)
                let! _record = Table.merge info.ConfigurationId info.ConfigurationId.RuntimeTable record
                return () 
            }
        
        member this.FaultInfo : JobFaultInfo = 
            // TODO : implement
            NoFault
        
        member this.GetJob() : Async<CloudJob> = 
            async { 
                let blob = Blob.FromPath(info.ConfigurationId, info.ParentJobId, info.JobId)
                use! stream = blob.OpenRead()
                let _pos = 
                    match info.Offset with
                    | Some pos -> stream.Seek(pos, SeekOrigin.Begin)
                    | _ -> 0L
                return Configuration.Pickler.Deserialize<CloudJob>(stream)
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
    
    static member FromBrokeredMessage(config : ConfigurationId, message : BrokeredMessage) = 
        let tryGet name = 
            match message.Properties.TryGetValue(name) with
            | true, v -> Some(v :?> 'T)
            | false, _ -> None
        
        let affinity = tryGet AffinityPropertyName
        let deliveryCount = message.DeliveryCount
        let parentId = fromGuid (message.Properties.[ParentTaskIdPropertyName] :?> Guid)
        let streamPos = tryGet StreamOffsetPropertyName
        let lockToken = message.LockToken
        let body = message.GetBody<string>()
        let dequeueTime = DateTimeOffset.Now
        let info = {
            JobId = body
            ConfigurationId = config
            MessageLockId = lockToken
            ParentJobId = parentId
            Offset = streamPos
            TargetWorker = affinity
            DeliveryCount = deliveryCount
            DequeueTime = dequeueTime
        }
        new JobLeaseToken(info)

/// Local queue subscription client
[<AutoSerializable(false)>]
type internal Subscription (config : ConfigurationId, workerId : IWorkerId) = 
    let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config)

    do 
        let ns = cp.NamespaceClient
        let topic = config.RuntimeTopic
        let affinity = workerId.Id
        if not <| ns.SubscriptionExists(topic, affinity) then 
            let sd = new SubscriptionDescription(topic, affinity)
            sd.DefaultMessageTimeToLive <- MaxTTL
            sd.LockDuration <- MaxLockDuration
            sd.AutoDeleteOnIdle <- SubscriptionAutoDeleteInterval
            let filter = new SqlFilter(sprintf "%s = '%s'" AffinityPropertyName affinity)
            ns.CreateSubscription(sd, filter) |> ignore

    let sub = cp.SubscriptionClient(config.RuntimeTopic, workerId.Id)
    
    member this.WorkerId = workerId

    member this.TryDequeue() : Async<JobLeaseToken option> = 
        async { 
            let! msg = sub.ReceiveAsync(ServerWaitTime)
            if msg = null then 
                return None
            else 
                let! _ = JobLeaseMonitor.Start(msg)
                return Some(JobLeaseToken.FromBrokeredMessage(config, msg))
        }

/// Local queue topic client
[<AutoSerializable(false)>]
type internal Topic (config : ConfigurationId) = 
    let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config)
    let topic = cp.TopicClient(config.RuntimeTopic)
    
    member this.GetSubscription(workerId : IWorkerId) : Subscription = new Subscription(config, workerId)
    
    member this.EnqueueBatch(jobs : CloudJob []) : Async<int64 seq> = 
        async { 
            //logger.Logf "Creating common job file."
            let ys = new ResizeArray<BrokeredMessage>(jobs.Length)
            let sizes = new ResizeArray<int64>(jobs.Length)
            for parentId, jobs in Seq.groupBy (fun j -> j.TaskEntry.Id) jobs do
                let temp = Path.GetTempFileName()
                let fileStream = File.OpenWrite(temp)
                let blobName = guid()
                let lastPosition = ref 0L
                for job in jobs do
                    Configuration.Pickler.Serialize(fileStream, job, leaveOpen = true)
                    let msg = new BrokeredMessage(blobName)
                    msg.Properties.Add(ParentTaskIdPropertyName, toGuid job.TaskEntry.Id)
                    msg.Properties.Add(AffinityPropertyName, job.TargetWorker.Value.Id)
                    msg.Properties.Add(StreamOffsetPropertyName, lastPosition.Value)
                    ys.Add(msg)
                    sizes.Add(fileStream.Position - lastPosition.Value)
                    lastPosition := fileStream.Position
                fileStream.Flush()
                fileStream.Dispose()

                //logger.Logf "Uploading job file [%s]." (getHumanReadableByteSize lastPosition.Value)
                do! Blob.UploadFromFile(config, parentId, blobName, temp)
                    |> Async.Ignore
                File.Delete(temp)

            do! topic.SendBatchAsync(ys)
            return sizes :> seq<int64>
        }
    
    member this.Enqueue(t : CloudJob) = 
        async { 
            let! blob = Blob.Create(config, t.TaskEntry.Id, t.Id, fun () -> t)
            let msg = new BrokeredMessage(t.Id)
            msg.Properties.Add(AffinityPropertyName, t.TargetWorker.Value.Id)
            msg.Properties.Add(ParentTaskIdPropertyName, toGuid t.TaskEntry.Id)
            do! topic.SendAsync(msg)
            return blob.Size
        }

    static member Create(config) = 
        async { 
            let ns = ConfigurationRegistry.Resolve<StoreClientProvider>(config).NamespaceClient
            let! exists = ns.TopicExistsAsync(config.RuntimeTopic)
            if not exists then 
                //logger.Logf "Creating Topic '%s'" config.RuntimeTopic
                let qd = new TopicDescription(config.RuntimeTopic)
                qd.EnableBatchedOperations <- true
                qd.EnablePartitioning <- true
                qd.DefaultMessageTimeToLive <- MaxTTL
                qd.UserMetadata <- Metadata.toString ReleaseInfo.localVersion config
                do! ns.CreateTopicAsync(qd)
            else
                //logger.Logf "Topic '%s' exists." config.RuntimeTopic
                ()
            return new Topic(config)
        }

/// Queue client implementation
[<AutoSerializable(false)>]
type internal Queue (config : ConfigurationId) = 
    let queue = ConfigurationRegistry.Resolve<StoreClientProvider>(config).QueueClient(config.RuntimeQueue, ReceiveMode.PeekLock)

    member this.EnqueueBatch(jobs : CloudJob []) = 
        async { 
            //logger.Logf "Creating common job file."
            let ys = new ResizeArray<BrokeredMessage>(jobs.Length)
            let sizes = new ResizeArray<int64>(jobs.Length)
            for parentId, jobs in Seq.groupBy (fun j -> j.TaskEntry.Id) jobs do
                let temp = Path.GetTempFileName()
                let fileStream = File.OpenWrite(temp)
                let blobName = guid()
                let lastPosition = ref 0L
                for job in jobs do
                    Configuration.Pickler.Serialize(fileStream, job, leaveOpen = true)
                    let msg = new BrokeredMessage(blobName)
                    msg.Properties.Add(ParentTaskIdPropertyName, toGuid job.TaskEntry.Id)
                    msg.Properties.Add(StreamOffsetPropertyName, lastPosition.Value)
                    ys.Add(msg)
                    sizes.Add(fileStream.Position - lastPosition.Value)
                    lastPosition := fileStream.Position
                fileStream.Flush()
                fileStream.Dispose()

                //logger.Logf "Uploading job file [%s]." (getHumanReadableByteSize lastPosition.Value)
                do! Blob.UploadFromFile(config, parentId, blobName, temp)
                    |> Async.Ignore
                File.Delete(temp)

            do! queue.SendBatchAsync(ys)
            return sizes :> seq<int64>
        }
    
    member this.Enqueue(job : CloudJob) = 
        async {
            let parentTaskId = job.TaskEntry.Id
            let! blob = Blob.Create(config, parentTaskId, job.Id, fun () -> job)
            let msg = new BrokeredMessage(toGuid job.Id)
            msg.Properties.Add(ParentTaskIdPropertyName, toGuid parentTaskId)
            do! queue.SendAsync(msg)
            return blob.Size
        }
    
    member this.TryDequeue() : Async<JobLeaseToken option> = 
        async { 
            let! msg = queue.ReceiveAsync(ServerWaitTime)
            if msg = null then 
                return None
            else
                let! _ = JobLeaseMonitor.Start(msg)
                return Some(JobLeaseToken.FromBrokeredMessage(config, msg))
        }
    
    static member Create(config : ConfigurationId) = 
        async { 
            let ns = ConfigurationRegistry.Resolve<StoreClientProvider>(config).NamespaceClient
            let! exists = ns.QueueExistsAsync(config.RuntimeQueue)
            if not exists then 
                //logger.Logf "Creating Queue '%s'" config.RuntimeQueue
                let qd = new QueueDescription(config.RuntimeQueue)
                qd.EnableBatchedOperations <- true
                qd.EnablePartitioning <- true
                qd.DefaultMessageTimeToLive <- MaxTTL
                qd.LockDuration <- MaxLockDuration
                qd.UserMetadata <- Metadata.toString ReleaseInfo.localVersion config
                do! ns.CreateQueueAsync(qd)
            else
                //logger.Logf "Queue '%s' exists." config.RuntimeQueue
                ()
            return new Queue(config)//, logger)
        }