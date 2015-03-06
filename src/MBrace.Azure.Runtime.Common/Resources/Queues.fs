namespace MBrace.Azure.Runtime.Resources

open System
open System.Runtime.Serialization
open Microsoft.ServiceBus.Messaging
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common
open MBrace.Azure

[<AutoOpenAttribute>]
module private Constants = 
    let RenewLockInverval = 10000
    let MaxLockDuration = TimeSpan.FromMilliseconds(3. * float RenewLockInverval)
    let MaxTTL = TimeSpan.MaxValue
    let ServerWaitTime = TimeSpan.FromMilliseconds(100.)
    let AffinityPropertyName = "Affinity"
    let PIDPropertyName = "ProcessId"
    let SubscriptionAutoDeleteInterval = TimeSpan.MaxValue 

/// Local wrapper for Service Bus message
[<AutoSerializable(false)>]
type QueueMessage(config : ConfigurationId, msg : BrokeredMessage) = 
    let mutable completed = false
    let affinity = 
        match msg.Properties.TryGetValue(AffinityPropertyName) with
        | false, _ -> None
        | true, affinity -> Some (string affinity)

    member __.Affinity = affinity

    member __.DeliveryCount = msg.DeliveryCount

    member __.ProcessId : string option =
        match msg.Properties.TryGetValue(PIDPropertyName) with
        | true, pid -> Some(pid :?> string)
        | false, _ -> None

    member this.CompleteAsync() = 
        async { 
            completed <- true
            do! msg.CompleteAsync()
        }
    
    member this.AbandonAsync() = 
        async { 
            completed <- true
            do! msg.AbandonAsync()
        }
    
    member __.RenewLoopAsync() = 
        let rec loop() = 
            async { 
                if completed then return ()
                else 
                    do! msg.RenewLockAsync()
                    do! Async.Sleep RenewLockInverval
                    return! loop()
            }
        loop()
    
    member __.GetPayloadAsync<'T>() : Async<'T> = 
        async { 
            let p = msg.GetBody<string>()
            let t = Blob.FromPath(config, p)
            return! t.GetValue()
        }

/// Local queue subscription client
[<AutoSerializable(false)>]
type internal Subscription (config : ConfigurationId, affinity : string) = 
    let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config)
    let subscription = sprintf "%s_%s" config.RuntimeTopic affinity

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
    member __.TryDequeue<'T>() : Async<QueueMessage option> = 
        async { 
            let! msg = sub.ReceiveAsync(ServerWaitTime)
            if msg = null then return None
            else return Some(QueueMessage(config, msg))
        }

/// Local queue topic client
[<AutoSerializable(false)>]
type internal Topic (config : ConfigurationId) = 
    let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config)
    let tc = cp.TopicClient(config.RuntimeTopic)
    member __.GetSubscription(affinity) : Subscription = new Subscription(config, affinity)
    
    member __.EnqueueBatch<'T>(xs : ('T * string) [], ?pid) = 
        async { 
            let! ys = xs
                      |> Array.map (fun (x, affinity) -> 
                             async { 
                                 let! bc = Blob.Create(config, defaultArg pid "jobs", guid(), fun () -> x)
                                 let msg = new BrokeredMessage(bc.Path)
                                 msg.Properties.Add(AffinityPropertyName, affinity)
                                 pid |> Option.iter(fun pid -> msg.Properties.Add(PIDPropertyName, pid))
                                 return msg
                             })
                      |> Async.Parallel
            do! tc.SendBatchAsync(ys)
        }

    member __.EnqueueBatch<'T>(xs : 'T [], affinity : string, ?pid) = 
        async { 
            let! ys = xs
                      |> Array.map (fun x -> 
                             async { 
                                 let! bc = Blob.Create(config, defaultArg pid "jobs", guid(), fun () -> x)
                                 let msg = new BrokeredMessage(bc.Path)
                                 msg.Properties.Add(AffinityPropertyName, affinity)
                                 pid |> Option.iter(fun pid -> msg.Properties.Add(PIDPropertyName, pid))
                                 return msg
                             })
                      |> Async.Parallel
            do! tc.SendBatchAsync(ys)
        }
    
    member __.Enqueue<'T>(t : 'T, affinity : string, ?pid) = 
        async { 
            let! bc = Blob.Create(config, defaultArg pid "jobs", guid(), fun () -> t)
            let msg = new BrokeredMessage(bc.Path)
            msg.Properties.Add(AffinityPropertyName, affinity)
            pid |> Option.iter(fun pid -> msg.Properties.Add(PIDPropertyName, pid))
            do! tc.SendAsync(msg)
        }

    static member Create(config) = 
        async { 
            let ns = ConfigurationRegistry.Resolve<StoreClientProvider>(config).NamespaceClient
            let name = config.RuntimeTopic
            if not <| ns.TopicExists(name) then 
                let td = new TopicDescription(name)
                td.EnableBatchedOperations <- true
                td.EnablePartitioning <- true
                td.DefaultMessageTimeToLive <- MaxTTL 
                do! ns.CreateTopicAsync(td)
            return new Topic(config)
        }

/// Queue client implementation
[<AutoSerializable(false)>]
type internal Queue (config : ConfigurationId) = 
    let queue = ConfigurationRegistry.Resolve<StoreClientProvider>(config).QueueClient(config.RuntimeQueue, ReceiveMode.PeekLock)
    let ns = ConfigurationRegistry.Resolve<StoreClientProvider>(config).NamespaceClient
    
    member __.Length = ns.GetQueue(config.RuntimeQueue).MessageCount
    
    member __.EnqueueBatch<'T>(xs : 'T [], ?pid) = 
        async { 
            let! ys = xs
                      |> Array.map (fun x -> async { let! bc = Blob.Create(config, defaultArg pid "jobs", guid(), fun () -> x)
                                                     let msg = new BrokeredMessage(bc.Path)
                                                     pid |> Option.iter(fun pid -> msg.Properties.Add(PIDPropertyName, pid))
                                                     return msg })
                      |> Async.Parallel
            do! queue.SendBatchAsync(ys)
        }
    
    member __.Enqueue<'T>(t : 'T, ?pid) = 
        async { 
            let! bc = Blob.Create(config, defaultArg pid "jobs", guid(), fun () -> t)
            let msg = new BrokeredMessage(bc.Path)
            pid |> Option.iter(fun pid -> msg.Properties.Add(PIDPropertyName, pid))
            do! queue.SendAsync(msg)
        }
    
    member __.TryDequeue<'T>() : Async<QueueMessage option> = 
        async { 
            let! msg = queue.ReceiveAsync(ServerWaitTime)
            if msg = null then return None
            else return Some(QueueMessage(config, msg))
        }
    
    static member Create(config : ConfigurationId) = 
        async { 
            let ns = ConfigurationRegistry.Resolve<StoreClientProvider>(config).NamespaceClient
            let qd = new QueueDescription(config.RuntimeQueue)
            qd.EnableBatchedOperations <- true
            qd.EnablePartitioning <- true
            qd.DefaultMessageTimeToLive <- MaxTTL
            qd.LockDuration <- MaxLockDuration
            let! exists = ns.QueueExistsAsync(config.RuntimeQueue)
            if not exists then do! ns.CreateQueueAsync(qd)
            return new Queue(config)
        }

// Unified access to Queue/Topic/Subscription.
[<AutoSerializable(false)>]
type JobQueue internal (queue : Queue, topic : Topic) = 
    let mutable affinity : string option = None
    let mutable subscription : Subscription option = None 
    let mutable flag = false

    member __.Affinity 
        with get () = affinity.Value
        and set aff = 
            affinity <- Some aff
            subscription <- Some(topic.GetSubscription(aff))


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
    static member Create(config : ConfigurationId) =
        async {
            let! queue = Queue.Create(config)
            let! topic = Topic.Create(config)
            return new JobQueue(queue, topic)
        }
