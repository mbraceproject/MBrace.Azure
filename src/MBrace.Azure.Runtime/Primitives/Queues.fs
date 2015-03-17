namespace MBrace.Azure.Runtime.Primitives

open System
open System.Runtime.Serialization
open Microsoft.ServiceBus.Messaging
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities
open MBrace.Azure
open MBrace.Runtime

[<AutoOpenAttribute>]
module private Helpers = 
    let RenewLockInverval = 10000
    let MaxLockDuration = TimeSpan.FromMilliseconds(3. * float RenewLockInverval)
    let MaxTTL = TimeSpan.MaxValue
    let ServerWaitTime = TimeSpan.FromMilliseconds(50.)
    let AffinityPropertyName = "Affinity"
    let ProcessIdPropertyName = "ProcessId"
    let SubscriptionAutoDeleteInterval = TimeSpan.MaxValue 

    let rec renewLoop (message : BrokeredMessage) = async {
        try
            do! message.RenewLockAsync()
        with :? MessageLockLostException ->
            return ()
        do! Async.Sleep RenewLockInverval
        return! renewLoop message
    }


    let partitionMessages(messages : BrokeredMessage seq) =
        let limit = 256L * 1024L
        let results = new ResizeArray<ResizeArray<BrokeredMessage>>()
        let mutable currentSize = 0L
        let mutable currentBatch = new ResizeArray<BrokeredMessage>()
        for m in messages do
            if m.Size + currentSize >= limit then
                results.Add(currentBatch)
                currentBatch <- new ResizeArray<BrokeredMessage>()
                currentSize <- 0L
            currentSize <- m.Size + currentSize
            currentBatch.Add(m)
        results


/// Local wrapper for Service Bus message
type QueueMessage(config : ConfigurationId, affinity, deliveryCount, processId, lockToken, body, isQueueMessage) = 

    member __.IsQueueMessage = isQueueMessage

    member __.LockToken = lockToken

    member __.Affinity = affinity

    member __.DeliveryCount = deliveryCount

    member __.ProcessId = processId
    
    member __.GetPayloadAsync<'T>() : Async<'T> = 
        async { 
            let t = Blob.FromPath(config, body)
            return! t.GetValue()
        }

    static member FromBrokeredMessage(config : ConfigurationId, message : BrokeredMessage, isQueueMessage) =
        let affinity = 
            match message.Properties.TryGetValue(AffinityPropertyName) with
            | true, aff -> Some(aff :?> string)
            | false, _  -> None
        let deliveryCount = message.DeliveryCount
        let processId = 
            match message.Properties.TryGetValue(AffinityPropertyName) with
            | true, aff -> Some(aff :?> string)
            | false, _  -> None
        let lockToken = message.LockToken
        let body = message.GetBody<string>()
        new QueueMessage(config, affinity, deliveryCount, processId, lockToken, body, isQueueMessage)

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

    member __.CompleteAsync(message : QueueMessage) =
        async {
            do! sub.CompleteAsync(message.LockToken)
        }

    member __.AbandonAsync(message : QueueMessage) =
        async {
            do! sub.AbandonAsync(message.LockToken)
        }
    
    member __.TryDequeue<'T>() : Async<QueueMessage option> = 
        async { 
            let! msg = sub.ReceiveAsync(ServerWaitTime)
            if msg = null then 
                return None
            else 
                do! Async.StartChild(renewLoop msg) |> Async.Ignore 
                return Some(QueueMessage.FromBrokeredMessage(config, msg, false))
        }

/// Local queue topic client
[<AutoSerializable(false)>]
type internal Topic (config : ConfigurationId, logger : ICloudLogger) = 
    let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config)
    let tc = cp.TopicClient(config.RuntimeTopic)
    member __.GetSubscription(affinity) : Subscription = new Subscription(config, logger, affinity)
    
    member __.EnqueueBatch<'T>(xs : ('T * string) [], ?pid) = 
        async { 
            let! ys = xs
                      |> Array.map (fun (x, affinity) -> 
                             async { 
                                 let! bc = Blob.Create(config, defaultArg pid "jobs", guid(), fun () -> x)
                                 let msg = new BrokeredMessage(bc.Path)
                                 msg.Properties.Add(AffinityPropertyName, affinity)
                                 pid |> Option.iter(fun pid -> msg.Properties.Add(ProcessIdPropertyName, pid))
                                 return msg
                             })
                      |> Async.Parallel
            do! ys |> partitionMessages
                   |> Seq.map(fun ms -> Async.AwaitTask(tc.SendBatchAsync(ms)))
                   |> Async.Parallel
                   |> Async.Ignore
        }
    
    member __.Enqueue<'T>(t : 'T, affinity : string, ?pid) = 
        async { 
            let! bc = Blob.Create(config, defaultArg pid "jobs", guid(), fun () -> t)
            let msg = new BrokeredMessage(bc.Path)
            msg.Properties.Add(AffinityPropertyName, affinity)
            pid |> Option.iter(fun pid -> msg.Properties.Add(ProcessIdPropertyName, pid))
            do! tc.SendAsync(msg)
        }

    static member Create(config, logger) = 
        async { 
            let ns = ConfigurationRegistry.Resolve<StoreClientProvider>(config).NamespaceClient
            let name = config.RuntimeTopic
            if not <| ns.TopicExists(name) then 
                let td = new TopicDescription(name)
                td.EnableBatchedOperations <- true
                td.EnablePartitioning <- true
                td.DefaultMessageTimeToLive <- MaxTTL 
                do! ns.CreateTopicAsync(td)
            return new Topic(config, logger)
        }

/// Queue client implementation
[<AutoSerializable(false)>]
type internal Queue (config : ConfigurationId, logger) = 
    let queue = ConfigurationRegistry.Resolve<StoreClientProvider>(config).QueueClient(config.RuntimeQueue, ReceiveMode.PeekLock)
    let ns = ConfigurationRegistry.Resolve<StoreClientProvider>(config).NamespaceClient
    
    member __.Length = ns.GetQueue(config.RuntimeQueue).MessageCount

    member __.CompleteAsync(message : QueueMessage) =
        async {
            do! queue.CompleteAsync(message.LockToken)
        }

    member __.AbandonAsync(message : QueueMessage) =
        async {
            do! queue.AbandonAsync(message.LockToken)
        }

    member __.EnqueueBatch<'T>(xs : 'T [], ?pid) = 
        async { 
            let! ys = xs
                      |> Array.map (fun x -> async { let! bc = Blob.Create(config, defaultArg pid "jobs", guid(), fun () -> x)
                                                     let msg = new BrokeredMessage(bc.Path)
                                                     pid |> Option.iter(fun pid -> msg.Properties.Add(ProcessIdPropertyName, pid))
                                                     return msg })
                      |> Async.Parallel
            do! ys |> partitionMessages
                   |> Seq.map(fun ms -> Async.AwaitTask(queue.SendBatchAsync(ms)))
                   |> Async.Parallel
                   |> Async.Ignore
        }
    
    member __.Enqueue<'T>(t : 'T, ?pid) = 
        async { 
            let! bc = Blob.Create(config, defaultArg pid "jobs", guid(), fun () -> t)
            let msg = new BrokeredMessage(bc.Path)
            pid |> Option.iter(fun pid -> msg.Properties.Add(ProcessIdPropertyName, pid))
            do! queue.SendAsync(msg)
        }
    
    member __.TryDequeue<'T>() : Async<QueueMessage option> = 
        async { 
            let! msg = queue.ReceiveAsync(ServerWaitTime)
            if msg = null then 
                return None
            else
                do! Async.StartChild(renewLoop msg) |> Async.Ignore 
                return Some(QueueMessage.FromBrokeredMessage(config, msg, true))
        }
    
    static member Create(config : ConfigurationId, logger) = 
        async { 
            let ns = ConfigurationRegistry.Resolve<StoreClientProvider>(config).NamespaceClient
            let qd = new QueueDescription(config.RuntimeQueue)
            qd.EnableBatchedOperations <- true
            qd.EnablePartitioning <- true
            qd.DefaultMessageTimeToLive <- MaxTTL
            qd.LockDuration <- MaxLockDuration
            let! exists = ns.QueueExistsAsync(config.RuntimeQueue)
            if not exists then do! ns.CreateQueueAsync(qd)
            return new Queue(config, logger)
        }

// Unified access to Queue/Topic/Subscription.
[<AutoSerializable(false)>]
type JobQueue internal (queue : Queue, topic : Topic, logger) = 
    let mutable affinity : string option = None
    let mutable subscription : Subscription option = None 
    let mutable flag = false

    member __.Affinity 
        with get () = affinity.Value
        and set aff = 
            affinity <- Some aff
            subscription <- Some(topic.GetSubscription(aff))

    member __.CompleteAsync(message : QueueMessage) =
        async {
            if message.IsQueueMessage then do! queue.CompleteAsync(message)
            else do! subscription.Value.CompleteAsync(message)
        }

    member __.AbandonAsync(message : QueueMessage) =
        async {
            if message.IsQueueMessage then do! queue.AbandonAsync(message)
            else do! subscription.Value.AbandonAsync(message)
        }

    member __.TryDequeue() : Async<QueueMessage option> =
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

    member __.Enqueue<'T>(item : 'T, ?affinity, ?pid : string) : Async<unit> =
        async {
            match affinity with
            | None -> return! queue.Enqueue<'T>(item, ?pid = pid)
            | Some affinity -> return! topic.Enqueue<'T>(item, affinity, ?pid = pid)
        }
    member __.EnqueueBatch<'T>(xs : 'T [], ?pid : string) = queue.EnqueueBatch<'T>(xs, ?pid = pid)

    member __.EnqueueBatch<'T>(xs : ('T * string option) [], ?pid : string) = async {
            let queueTasks = new ResizeArray<'T>()
            let topicTasks = new ResizeArray<'T * string>()

            for (t,a) in xs do
                match a with
                | Some a -> topicTasks.Add(t,a)
                | None -> queueTasks.Add(t)
            
            let! handle1 = 
                if topicTasks.Count = 0 then async.Zero() 
                else topic.EnqueueBatch(topicTasks.ToArray(), ?pid = pid)
                |> Async.StartChild
            let! handle2 =
                if queueTasks.Count = 0 then async.Zero()
                else queue.EnqueueBatch(queueTasks.ToArray(), ?pid = pid)
                |> Async.StartChild
            do! Async.Parallel [handle1 ; handle2]
                |> Async.Ignore
        }
    
    /// Yadda Yadda
    static member Create(config : ConfigurationId, logger) =
        async {
            let! queue = Queue.Create(config, logger)
            let! topic = Topic.Create(config, logger)
            return new JobQueue(queue, topic, logger)
        }
