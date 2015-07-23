namespace MBrace.Azure.Runtime

open System
open System.Runtime.Serialization
open Microsoft.ServiceBus.Messaging
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities
open MBrace.Azure
open MBrace.Core.Internals
open System.Threading.Tasks
open System.IO
open MBrace.Runtime.Utils
open MBrace.Azure.Runtime.Primitives
open MBrace.Runtime
open Nessos.FsPickler.ExtensionMethods

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

[<Sealed; AbstractClass>]
type JobLeaseMonitor private () = 
    
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
    
    static member Complete(token : JobLeaseToken) : Async<unit> = 
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

and JobLeaseToken private (configurationId : ConfigurationId, jobId : string, 
                            messageLockId : Guid, parentJobId : string, 
                            offset : int64 option, targetWorker : string option, 
                            deliveryCount : int) = 

    member this.ConfigurationId = configurationId
    member this.TargetWorker = targetWorker
    member this.MessageLockId = messageLockId
    member this.DeliveryCount = deliveryCount
    
    interface ICloudJobLeaseToken with
        member this.DeclareCompleted() : Async<unit> = JobLeaseMonitor.Complete(this)
        
        member this.DeclareFaulted(arg1 : ExceptionDispatchInfo) : Async<unit> = 
            async { 
                // TODO : Update record
                return! JobLeaseMonitor.Complete(this) }
        
        member this.FaultInfo : JobFaultInfo = failwith "Not implemented yet"
        
        member this.GetJob() : Async<CloudJob> = 
            async { 
                let blob = Blob.FromPath(configurationId, parentJobId, jobId)
                use! stream = blob.OpenRead()
                let _pos = 
                    match offset with
                    | Some pos -> stream.Seek(pos, SeekOrigin.Begin)
                    | _ -> 0L
                return Configuration.Pickler.Deserialize<CloudJob>(stream)
            }
        
        member this.Id : string = jobId
        
        member this.JobType : JobType = failwith "Not implemented yet"
        
        member this.Size : int64 = failwith "Not implemented yet"
        
        member this.TargetWorker : IWorkerId option = 
            match targetWorker with
            | None -> None
            | Some w -> Some(WorkerId(w) :> _)
        
        member this.TaskEntry : ICloudTaskCompletionSource = TaskCompletionSource(configurationId, parentJobId) :> _
        
        member this.Type : string = failwith "Not implemented yet"
    
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
        new JobLeaseToken(config, body, lockToken, parentId, streamPos, affinity, deliveryCount)


/// Local queue subscription client
[<AutoSerializable(false)>]
type internal Subscription (config : ConfigurationId, affinity : string) = 
    let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config)

    do 
        let ns = cp.NamespaceClient
        let topic = config.RuntimeTopic
        if not <| ns.SubscriptionExists(topic, affinity) then 
            let sd = new SubscriptionDescription(topic, affinity)
            sd.DefaultMessageTimeToLive <- MaxTTL
            sd.LockDuration <- MaxLockDuration
            sd.AutoDeleteOnIdle <- SubscriptionAutoDeleteInterval
            let filter = new SqlFilter(sprintf "%s = '%s'" AffinityPropertyName affinity)
            ns.CreateSubscription(sd, filter) |> ignore

    let sub = cp.SubscriptionClient(config.RuntimeTopic, affinity)
    
    member this.TryDequeue<'T>() : Async<ICloudJobLeaseToken option> = 
        async { 
            let! msg = sub.ReceiveAsync(ServerWaitTime)
            if msg = null then 
                return None
            else 
                let! _ = JobLeaseMonitor.Start(msg)
                return Some(JobLeaseToken.FromBrokeredMessage(config, msg) :> ICloudJobLeaseToken)
        }

/// Local queue topic client
[<AutoSerializable(false)>]
type internal Topic (config : ConfigurationId) = 
    let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config)
    let topic = cp.TopicClient(config.RuntimeTopic)
    
    member this.Metadata = cp.NamespaceClient.GetTopic(config.RuntimeTopic).UserMetadata
    
    member this.GetSubscription(affinity) : Subscription = new Subscription(config, affinity)
    
    member this.EnqueueBatch(jobs : CloudJob []) = 
        async { 
            //logger.Logf "Creating common job file."
            let ys = new ResizeArray<BrokeredMessage>(jobs.Length)
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
                    lastPosition := fileStream.Position
                fileStream.Flush()
                fileStream.Dispose()

                //logger.Logf "Uploading job file [%s]." (getHumanReadableByteSize lastPosition.Value)
                do! Blob.UploadFromFile(config, parentId, blobName, temp)
                    |> Async.Ignore
                File.Delete(temp)

            do! topic.SendBatchAsync(ys)
        }
    
    member this.Enqueue(t : CloudJob) = 
        async { 
            let! _blob = Blob.Create(config, t.TaskEntry.Id, t.Id, fun () -> t)
            let msg = new BrokeredMessage(t.Id)
            msg.Properties.Add(AffinityPropertyName, t.TargetWorker.Value.Id)
            msg.Properties.Add(ParentTaskIdPropertyName, toGuid t.TaskEntry.Id)
            do! topic.SendAsync(msg)
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
    let ns = ConfigurationRegistry.Resolve<StoreClientProvider>(config).NamespaceClient
    
    member this.Length = ns.GetQueue(config.RuntimeQueue).MessageCount

    member this.Metadata = ns.GetQueue(config.RuntimeQueue).UserMetadata

    member this.EnqueueBatch(jobs : CloudJob []) = 
        async { 
            //logger.Logf "Creating common job file."
            let ys = new ResizeArray<BrokeredMessage>(jobs.Length)
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
                    lastPosition := fileStream.Position
                fileStream.Flush()
                fileStream.Dispose()

                //logger.Logf "Uploading job file [%s]." (getHumanReadableByteSize lastPosition.Value)
                do! Blob.UploadFromFile(config, parentId, blobName, temp)
                    |> Async.Ignore
                File.Delete(temp)

            return! queue.SendBatchAsync(ys)
        }
    
    member this.Enqueue(job : CloudJob) = 
        async {
            let parentTaskId = job.TaskEntry.Id
            let! _blob = Blob.Create(config, parentTaskId, job.Id, fun () -> job)
            let msg = new BrokeredMessage(toGuid job.Id)
            msg.Properties.Add(ParentTaskIdPropertyName, toGuid parentTaskId)
            do! queue.SendAsync(msg)
        }
    
    member this.TryDequeue<'T>() : Async<ICloudJobLeaseToken option> = 
        async { 
            let! msg = queue.ReceiveAsync(ServerWaitTime)
            if msg = null then 
                return None
            else
                let! _ = JobLeaseMonitor.Start(msg)
                return Some(JobLeaseToken.FromBrokeredMessage(config, msg) :> ICloudJobLeaseToken)
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