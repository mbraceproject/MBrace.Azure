namespace MBrace.Azure.Runtime.Primitives

open System
open System.Runtime.Serialization
open Microsoft.ServiceBus.Messaging
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities
open MBrace.Azure
open MBrace.Runtime
open System.Threading.Tasks
open System.IO

[<AutoOpenAttribute>]
module private Helpers = 
    let RenewLockInverval = 10000
    let MaxLockDuration = TimeSpan.FromMinutes(5.)
    let MaxTTL = TimeSpan.MaxValue
    let ServerWaitTime = TimeSpan.FromMilliseconds(50.)
    let AffinityPropertyName = "Affinity"
    let ProcessIdPropertyName = "ProcessId"
    let StreamOffsetPropertyName = "StreamOffset"
    let SubscriptionAutoDeleteInterval = TimeSpan.MaxValue 

    let rec renewLoop (message : BrokeredMessage) = async {
        try
            do! message.RenewLockAsync()
        with
        | :? MessageLockLostException -> return ()
        | _ -> ()
        do! Async.Sleep RenewLockInverval
        return! renewLoop message
    }

    let partitionMessages(messages : BrokeredMessage seq) =
        let limit = 256L * 1024L // 256K batch limit
        // total size cannot be calculated by .Size; add a min size to hold properties
        // ~130 empty message size, 2 bytes offset, 64 bytes affinity
        let minMessageSize = 256L (* 140L + 2L + 1L + 64L *)
        let results = new ResizeArray<ResizeArray<BrokeredMessage>>()
        let mutable currentSize = 0L
        let mutable currentBatch = new ResizeArray<BrokeredMessage>()
        for m in messages do
            let messageSize = minMessageSize (* + m.Size *)
            if messageSize + currentSize >= limit then
                results.Add(currentBatch)
                currentBatch <- new ResizeArray<BrokeredMessage>()
                currentSize <- 0L
            currentSize <- messageSize + currentSize
            currentBatch.Add(m)
        if currentSize > 0L then results.Add(currentBatch)
        results


/// Local wrapper for Service Bus message
type QueueMessage(config : ConfigurationId, affinity, deliveryCount, processId, lockToken, body, streamOffset) = 

    member this.IsQueueMessage = this.Affinity.IsNone

    member this.LockToken = lockToken

    member this.Affinity : string option = affinity

    member this.DeliveryCount = deliveryCount

    member this.ProcessId = processId
    
    member this.StreamOffset = streamOffset

    member this.GetPayloadAsync<'T>() : Async<'T> = 
        async { 
            let t = Blob.FromPath(config, processId, body)
            use! stream = t.OpenRead()
            stream.Seek(this.StreamOffset, SeekOrigin.Begin) |> ignore
            return Configuration.Pickler.Deserialize<'T>(stream)
        }

    static member FromBrokeredMessage(config : ConfigurationId, message : BrokeredMessage) =
        let tryGet name = 
            match message.Properties.TryGetValue(name) with
            | true, v -> Some(v :?> 'T)
            | false, _ -> None
        let affinity = tryGet AffinityPropertyName
        let deliveryCount = message.DeliveryCount
        let processId = fromGuid(message.Properties.[ProcessIdPropertyName] :?> Guid)
        let streamPos = Option.fold (fun _ p -> p) 0L (tryGet StreamOffsetPropertyName) 

        let lockToken = message.LockToken
        let body = message.GetBody<string>()
        new QueueMessage(config, affinity, deliveryCount, processId, lockToken, body, streamPos)

/// Local queue subscription client
[<AutoSerializable(false)>]
type internal Subscription (config : ConfigurationId, logger : ICloudLogger, affinity : string) = 
    let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config)
    let subscription = affinity

    do 
        let ns = cp.NamespaceClient
        let topic = config.RuntimeTopic
        if not <| ns.SubscriptionExists(topic, subscription) then 
            let sd = new SubscriptionDescription(topic, subscription)
            sd.DefaultMessageTimeToLive <- MaxTTL
            sd.LockDuration <- MaxLockDuration
            sd.AutoDeleteOnIdle <- SubscriptionAutoDeleteInterval
            let filter = new SqlFilter(sprintf "%s = '%s'" AffinityPropertyName affinity)
            ns.CreateSubscription(sd, filter) |> ignore

    let sub = cp.SubscriptionClient(config.RuntimeTopic, subscription)

    member this.CompleteAsync(message : QueueMessage) =
        async {
            do! sub.CompleteAsync(message.LockToken)
        }

    member this.AbandonAsync(message : QueueMessage) =
        async {
            do! sub.AbandonAsync(message.LockToken)
        }
    
    member this.TryDequeue<'T>() : Async<QueueMessage option> = 
        async { 
            let! msg = sub.ReceiveAsync(ServerWaitTime)
            if msg = null then 
                return None
            else 
                do! Async.StartChild(renewLoop msg) |> Async.Ignore 
                return Some(QueueMessage.FromBrokeredMessage(config, msg))
        }

/// Local queue topic client
[<AutoSerializable(false)>]
type internal Topic (config : ConfigurationId, logger : ICloudLogger) = 
    let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config)
    let tc = cp.TopicClient(config.RuntimeTopic)
    
    member this.Metadata = cp.NamespaceClient.GetTopic(config.RuntimeTopic).UserMetadata
    
    member this.GetSubscription(affinity) : Subscription = new Subscription(config, logger, affinity)
    
    member this.EnqueueBatch<'T>(xs : ('T * string) [], pid) = 
        async { 
            logger.Logf "Creating common job file."
            let temp = Path.GetRandomFileName()
            let fileStream = File.OpenWrite(temp)
            let blobName = guid()
            let lastPosition = ref 0L
            let ys = new ResizeArray<BrokeredMessage>(xs.Length)
            for x, affinity in xs do
                Configuration.Pickler.Serialize(fileStream, x, leaveOpen = true)
                let msg = new BrokeredMessage(blobName)
                msg.Properties.Add(ProcessIdPropertyName, toGuid pid)
                msg.Properties.Add(AffinityPropertyName, affinity)
                msg.Properties.Add(StreamOffsetPropertyName, lastPosition.Value)
                ys.Add(msg)
                lastPosition := fileStream.Position
            fileStream.Flush()
            fileStream.Dispose()

            logger.Logf "Uploading job file [%d bytes]." lastPosition.Value
            do! Blob.UploadFromFile(config, pid, blobName, temp)
                |> Async.Ignore

            let parts = partitionMessages ys
            logger.Logf "Queue EnqueueBatch %d messages : [%s]" xs.Length (parts |> Seq.map (Seq.length >> string) |> String.concat ", ")
            return! parts |> Seq.mapi(fun i ms -> async {
                            do! Async.AwaitTask(tc.SendBatchAsync(ms))
                            logger.Logf "Batch %d, length %d completed" i ms.Count })
                          |> Async.Parallel
                          |> Async.Ignore
        }
    
    member this.Enqueue<'T>(t : 'T, affinity : string, pid) = 
        async { 
            let name = guid()
            do! Blob.Create(config, pid, name, fun () -> t) |> Async.Ignore
            let msg = new BrokeredMessage(name)
            msg.Properties.Add(AffinityPropertyName, affinity)
            msg.Properties.Add(ProcessIdPropertyName, toGuid pid)
            do! tc.SendAsync(msg)
        }

    static member Create(config, logger : ICloudLogger) = 
        async { 
            let ns = ConfigurationRegistry.Resolve<StoreClientProvider>(config).NamespaceClient
            let! exists = ns.TopicExistsAsync(config.RuntimeTopic)
            if not exists then 
                logger.Logf "Creating Topic '%s'" config.RuntimeTopic
                let qd = new TopicDescription(config.RuntimeTopic)
                qd.EnableBatchedOperations <- true
                qd.EnablePartitioning <- true
                qd.DefaultMessageTimeToLive <- MaxTTL
                qd.UserMetadata <- ReleaseInfo.localVersion
                do! ns.CreateTopicAsync(qd)
            else
                logger.Logf "Topic '%s' exists." config.RuntimeTopic
            return new Topic(config, logger)
        }

/// Queue client implementation
[<AutoSerializable(false)>]
type internal Queue (config : ConfigurationId, logger : ICloudLogger) = 
    let queue = ConfigurationRegistry.Resolve<StoreClientProvider>(config).QueueClient(config.RuntimeQueue, ReceiveMode.PeekLock)
    let ns = ConfigurationRegistry.Resolve<StoreClientProvider>(config).NamespaceClient
    
    member this.Length = ns.GetQueue(config.RuntimeQueue).MessageCount

    member this.Metadata = ns.GetQueue(config.RuntimeQueue).UserMetadata

    member this.CompleteAsync(message : QueueMessage) =
        async {
            do! queue.CompleteAsync(message.LockToken)
        }

    member this.AbandonAsync(message : QueueMessage) =
        async {
            do! queue.AbandonAsync(message.LockToken)
        }

    member this.EnqueueBatch<'T>(xs : 'T [], pid) = 
        async { 
            logger.Logf "Creating common job file."
            let temp = Path.GetRandomFileName()
            let fileStream = File.OpenWrite(temp)
            let blobName = guid()
            let lastPosition = ref 0L
            let ys = new ResizeArray<BrokeredMessage>(xs.Length)
            for x in xs do
                Configuration.Pickler.Serialize(fileStream, x, leaveOpen = true)
                let msg = new BrokeredMessage(blobName)
                msg.Properties.Add(ProcessIdPropertyName, toGuid pid)
                msg.Properties.Add(StreamOffsetPropertyName, lastPosition.Value)
                ys.Add(msg)
                lastPosition := fileStream.Position
            fileStream.Flush()
            fileStream.Dispose()

            logger.Logf "Uploading job file [%d bytes]." lastPosition.Value
            do! Blob.UploadFromFile(config, pid, blobName, temp)
                |> Async.Ignore

            let parts = partitionMessages ys
            logger.Logf "Queue EnqueueBatch %d messages : %s" xs.Length (parts |> Seq.map (Seq.length >> string) |> String.concat ", ")
            return! parts |> Seq.mapi(fun i ms -> async {
                            do! Async.AwaitTask(queue.SendBatchAsync(ms))
                            logger.Logf "Batch %d, length %d completed." i ms.Count })
                          |> Async.Parallel
                          |> Async.Ignore
        }
    
    member this.Enqueue<'T>(t : 'T, pid) = 
        async { 
            let name = guid()
            do! Blob.Create(config, pid, name, fun () -> t) |> Async.Ignore
            let msg = new BrokeredMessage(name)
            msg.Properties.Add(ProcessIdPropertyName, toGuid pid)
            do! queue.SendAsync(msg)
        }
    
    member this.TryDequeue<'T>() : Async<QueueMessage option> = 
        async { 
            let! msg = queue.ReceiveAsync(ServerWaitTime)
            if msg = null then 
                return None
            else
                do! Async.StartChild(renewLoop msg) |> Async.Ignore 
                return Some(QueueMessage.FromBrokeredMessage(config, msg))
        }
    
    static member Create(config : ConfigurationId, logger : ICloudLogger) = 
        async { 
            let ns = ConfigurationRegistry.Resolve<StoreClientProvider>(config).NamespaceClient
            let! exists = ns.QueueExistsAsync(config.RuntimeQueue)
            if not exists then 
                logger.Logf "Creating Queue '%s'" config.RuntimeQueue
                let qd = new QueueDescription(config.RuntimeQueue)
                qd.EnableBatchedOperations <- true
                qd.EnablePartitioning <- true
                qd.DefaultMessageTimeToLive <- MaxTTL
                qd.LockDuration <- MaxLockDuration
                qd.UserMetadata <- ReleaseInfo.localVersion
                do! ns.CreateQueueAsync(qd)
            else
                logger.Logf "Queue '%s' exists." config.RuntimeQueue
            return new Queue(config, logger)
        }

// Unified access to Queue/Topic/Subscription.
[<AutoSerializable(false)>]
type JobQueue internal (queue : Queue, topic : Topic, logger) = 
    let mutable affinity : string option = None
    let mutable subscription : Subscription option = None 
    let mutable flag = false

    member this.Affinity 
        with get () = affinity.Value
        and set aff = 
            affinity <- Some aff
            subscription <- Some(topic.GetSubscription(aff))

    member this.CompleteAsync(message : QueueMessage) =
        async {
            if message.IsQueueMessage then do! queue.CompleteAsync(message)
            else do! subscription.Value.CompleteAsync(message)
        }

    member this.AbandonAsync(message : QueueMessage) =
        async {
            if message.IsQueueMessage then do! queue.AbandonAsync(message)
            else do! subscription.Value.AbandonAsync(message)
        }

    member this.TryDequeue() : Async<QueueMessage option> =
        async {
            let! msg = async {
                match flag, subscription with
                | _, None -> 
                    return! queue.TryDequeue()
                | false, Some subscription ->
                    let! msg = subscription.TryDequeue()
                    match msg with
                    | Some _ -> return msg
                    | None -> return! queue.TryDequeue()
                | true, Some subscription ->
                    let! msg = queue.TryDequeue()
                    match msg with
                    | Some _ -> return msg
                    | None -> return! subscription.TryDequeue() 
            }
            flag <- not flag
            return msg
        }

    member this.Enqueue<'T>(item : 'T, pid : string, ?affinity) : Async<unit> =
        async {
            match affinity with
            | None -> return! queue.Enqueue<'T>(item, pid)
            | Some affinity -> return! topic.Enqueue<'T>(item, affinity, pid)
        }
    
    member this.EnqueueBatch<'T>(xs : 'T [], pid : string) = queue.EnqueueBatch<'T>(xs, pid)

    member this.EnqueueBatch<'T>(xs : ('T * string option) [], pid : string) = async {
            let queueTasks = new ResizeArray<'T>()
            let topicTasks = new ResizeArray<'T * string>()

            for (t,a) in xs do
                match a with
                | Some a -> topicTasks.Add(t,a)
                | None -> queueTasks.Add(t)
            
            let! handle1 = 
                if topicTasks.Count = 0 then async.Zero() 
                else topic.EnqueueBatch(topicTasks.ToArray(), pid)
                |> Async.StartChild
            let! handle2 =
                if queueTasks.Count = 0 then async.Zero()
                else queue.EnqueueBatch(queueTasks.ToArray(), pid)
                |> Async.StartChild
            do! Async.Parallel [handle1 ; handle2]
                |> Async.Ignore
        }
    
    /// Get topic and queue versions.
    member this.Versions : string seq = [ queue.Metadata; topic.Metadata ] :> _

    /// Yadda Yadda
    static member Create(config : ConfigurationId, logger) =
        async {
            let! queue = Queue.Create(config, logger)
            let! topic = Topic.Create(config, logger)

            return new JobQueue(queue, topic, logger)
        }
