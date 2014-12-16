namespace Nessos.MBrace.Azure.Runtime.Resources

open System
open System.Runtime.Serialization
open Microsoft.ServiceBus.Messaging
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Common

[<AutoOpenAttribute>]
module private Constants = 
    let RenewLockInverval = 10000
    let MaxLockDuration = TimeSpan.FromMilliseconds(3. * float RenewLockInverval)
    let MaxTTL = TimeSpan.MaxValue
    let ServerWaitTime = TimeSpan.FromMilliseconds(100.)
    let AffinityPropertyName = "Affinity"
    let SubscriptionAutoDeleteInterval = TimeSpan.FromMinutes(5.) // 5min is the minimum duration
    //let SubscriptionRenewInterval = TimeSpan.FromSeconds(30.)

type QueueMessage(config, msg : BrokeredMessage, isQueueMessage : bool) = 
    let mutable completed = false
    let affinity = 
        match msg.Properties.TryGetValue(AffinityPropertyName) with
        | false, _ -> None
        | true, affinity -> Some (string affinity)

    member __.Affinity = affinity

    /// Defines if the message belongs to a Queue or a Topic.
    /// Queue messages with Affinity are messages that failed to Complete in a Subscription.
    member __.IsQueueMessage = isQueueMessage 

    member __.DeliveryCount = msg.DeliveryCount

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
            let p = msg.GetBody<Uri>()
            let t = BlobCell.OfUri<'T>(config, p)
            return! t.GetValue()
        }

type internal Subscription (config, topic, affinity : string, deadLetterPath : string) = 
    let cp = ConfigurationRegistry.Resolve<ClientProvider>(config)
    let subscription = sprintf "affinity_%s" affinity

    do 
        let ns = cp.NamespaceClient
        if not <| ns.SubscriptionExists(topic, subscription) then 
            let sd = new SubscriptionDescription(topic, subscription)
            sd.DefaultMessageTimeToLive <- MaxTTL
            sd.LockDuration <- MaxLockDuration
            sd.AutoDeleteOnIdle <- SubscriptionAutoDeleteInterval
            sd.ForwardDeadLetteredMessagesTo <- deadLetterPath
            let filter = new SqlFilter(sprintf "%s = '%s'" AffinityPropertyName affinity)
            ns.CreateSubscription(sd, filter) |> ignore
    
    let sub = cp.SubscriptionClient(topic, subscription)
    member __.TryDequeue<'T>() : Async<QueueMessage option> = 
        async { 
            let! msg = sub.ReceiveAsync(ServerWaitTime)
            if msg = null then return None
            else return Some(QueueMessage(config, msg, false))
        }

type internal Topic (config, topic) = 
    let cp = ConfigurationRegistry.Resolve<ClientProvider>(config)
    let ns = cp.NamespaceClient
    let tc = cp.TopicClient(topic)
    member __.Name = topic
    member __.GetSubscription(affinity, deadLetterPath) : Subscription = new Subscription(config, topic, affinity, deadLetterPath)
    
    member __.EnqueueBatch<'T>(xs : ('T * string) []) = 
        async { 
            let! ys = xs
                      |> Array.map (fun (x, affinity) -> 
                             async { 
                                 let! bc = BlobCell.CreateIfNotExists(config, topic, fun () -> x)
                                 let msg = new BrokeredMessage((bc :> IResource).Uri)
                                 msg.Properties.Add(AffinityPropertyName, affinity)
                                 return msg
                             })
                      |> Async.Parallel
            do! tc.SendBatchAsync(ys)
        }

    member __.EnqueueBatch<'T>(xs : 'T [], affinity : string) = 
        async { 
            let! ys = xs
                      |> Array.map (fun x -> 
                             async { 
                                 let! bc = BlobCell.CreateIfNotExists(config, topic, fun () -> x)
                                 let msg = new BrokeredMessage((bc :> IResource).Uri)
                                 msg.Properties.Add(AffinityPropertyName, affinity)
                                 return msg
                             })
                      |> Async.Parallel
            do! tc.SendBatchAsync(ys)
        }
    
    member __.Enqueue<'T>(t : 'T, affinity : string) = 
        async { 
            let! bc = BlobCell.CreateIfNotExists(config, topic, fun () -> t)
            let msg = new BrokeredMessage((bc :> IResource).Uri)
            msg.Properties.Add(AffinityPropertyName, affinity)
            do! tc.SendAsync(msg)
        }
    
    interface ISerializable with
        member x.GetObjectData(info : SerializationInfo, context : StreamingContext) : unit = 
            info.AddValue("topic", topic, typeof<string>)
            info.AddValue("config", config, typeof<ConfigurationId>)
    
    new(info : SerializationInfo, context : StreamingContext) = 
        let topic = info.GetValue("topic", typeof<string>) :?> string
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new Topic(config, topic)

    static member Create(config, name) = 
        async { 
            let ns = ConfigurationRegistry.Resolve<ClientProvider>(config).NamespaceClient
            if not <| ns.TopicExists(name) then 
                let td = new TopicDescription(name)
                td.EnableBatchedOperations <- true
                td.EnablePartitioning <- true
                td.DefaultMessageTimeToLive <- MaxTTL 
                do! ns.CreateTopicAsync(td)
            return new Topic(config, name)
        }

/// Queue implementation.
type internal Queue (config : ConfigurationId, res : Uri) = 
    let queue = ConfigurationRegistry.Resolve<ClientProvider>(config).QueueClient(res.Queue, ReceiveMode.PeekLock)
    let ns = ConfigurationRegistry.Resolve<ClientProvider>(config).NamespaceClient
    
    member __.Path = queue.Path
    
    member __.Length = ns.GetQueue(res.Queue).MessageCount
    
    member __.EnqueueBatch<'T>(xs : 'T []) = 
        async { 
            let! ys = xs
                      |> Array.map (fun x -> async { let! bc = BlobCell.CreateIfNotExists(config, res.Queue, fun () -> x)
                                                     return new BrokeredMessage((bc :> IResource).Uri) })
                      |> Async.Parallel
            do! queue.SendBatchAsync(ys)
        }
    
    member __.Enqueue<'T>(t : 'T) = 
        async { 
            let! bc = BlobCell.CreateIfNotExists(config, res.Queue, fun () -> t)
            let msg = new BrokeredMessage((bc :> IResource).Uri)
            do! queue.SendAsync(msg)
        }
    
    member __.TryDequeue<'T>() : Async<QueueMessage option> = 
        async { 
            let! msg = queue.ReceiveAsync(ServerWaitTime)
            if msg = null then return None
            else return Some(QueueMessage(config, msg, true))
        }
    
    interface IResource with
        member __.Uri = res
    
    interface ISerializable with
        member x.GetObjectData(info : SerializationInfo, context : StreamingContext) : unit = 
            info.AddValue("uri", res, typeof<Uri>)
            info.AddValue("config", config, typeof<ConfigurationId>)
    
    new(info : SerializationInfo, context : StreamingContext) = 
        let res = info.GetValue("uri", typeof<Uri>) :?> Uri
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new Queue(config, res)
    
    static member private GetUri(container) = uri "queue:%s" container
    static member Create(config, container : string) = 
        async { 
            let res = Queue.GetUri container
            let ns = ConfigurationRegistry.Resolve<ClientProvider>(config).NamespaceClient
            let qd = new QueueDescription(res.Queue)
            qd.EnableBatchedOperations <- true
            qd.EnablePartitioning <- true
            qd.DefaultMessageTimeToLive <- MaxTTL
            qd.LockDuration <- MaxLockDuration
            let! exists = ns.QueueExistsAsync(res.Queue)
            if not exists then do! ns.CreateQueueAsync(qd)
            return new Queue(config, res)
        }

// Unified access to Queue/Topic/Subscription.
type TaskQueue internal (config : ConfigurationId, queue : Queue, topic : Topic) = 
    let mutable affinity : string option = None
    let mutable subscription : Subscription option = None 

    member __.Affinity 
        with get () = affinity.Value
        and set aff = 
            affinity <- Some aff
            subscription <- Some(topic.GetSubscription(aff, queue.Path))

    member __.TryDequeue() : Async<QueueMessage option> =
        async {
            let! msg = subscription.Value.TryDequeue()
            match msg with
            | Some _ -> return msg
            | None -> return! queue.TryDequeue()
        }

    member __.Enqueue<'T>(item : 'T, ?affinity) : Async<unit> =
        async {
            match affinity with
            | None -> return! queue.Enqueue<'T>(item)
            | Some affinity -> return! topic.Enqueue<'T>(item, affinity)
        }
    member __.EnqueueBatch<'T>(xs : 'T []) = queue.EnqueueBatch<'T>(xs)

    //member __.EnqueueBatch<'T>(xs : 'T [], affinity : string) = topic.EnqueueBatch<'T>(xs, affinity)

    //member __.EnqueueBatch<'T>(xs : ('T * string) []) = topic.EnqueueBatch<'T>(xs)

    interface ISerializable with
        member x.GetObjectData(info : SerializationInfo, context : StreamingContext) : unit = 
            info.AddValue("queue", queue, typeof<Queue>)
            info.AddValue("topic", topic, typeof<Topic>)
            info.AddValue("config", config, typeof<ConfigurationId>)
    
    new(info : SerializationInfo, context : StreamingContext) = 
        let queue = info.GetValue("queue", typeof<Queue>) :?> Queue
        let topic = info.GetValue("topic", typeof<Topic>) :?> Topic
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new TaskQueue(config, queue, topic)

    static member Create(config, queue, topic) =
        async {
            let! queue = Queue.Create(config, queue)
            let! topic = Topic.Create(config, topic)
            return new TaskQueue(config, queue, topic)
        }
