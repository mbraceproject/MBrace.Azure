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

type QueueMessage(config, msg : BrokeredMessage) = 
    let completed = ref false

    member __.DeliveryCount = msg.DeliveryCount
    
    member __.CompleteAsync() = 
        async { 
            completed := true
            do! ofTask <| msg.CompleteAsync()
        }
    
    member __.AbandonAsync() = 
        async { 
            completed := true
            do! ofTask <| msg.AbandonAsync()
        }
    
    member __.RenewLoopAsync() = 
        let rec loop() = 
            async { 
                if !completed then return ()
                else 
                    do! ofTask <| msg.RenewLockAsync()
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

type Subscription internal (config, topic, affinity : string) = 
    let cp = ConfigurationRegistry.Resolve<ClientProvider>(config)
    let subscription = sprintf "affinity_%s" affinity

    do 
        let ns = cp.NamespaceClient
        if not <| ns.SubscriptionExists(topic, subscription) then 
            let sd = new SubscriptionDescription(topic, subscription)
            sd.DefaultMessageTimeToLive <- MaxTTL
            sd.LockDuration <- MaxLockDuration
            let filter = new SqlFilter(sprintf "%s = '%s'" AffinityPropertyName affinity)
            ns.CreateSubscription(sd, filter) |> ignore
    
    let sub = cp.SubscriptionClient(topic, subscription)
    member __.TryDequeue<'T>() : Async<QueueMessage option> = 
        async { 
            let! msg = sub.ReceiveAsync(ServerWaitTime)
            if msg = null then return None
            else return Some(QueueMessage(config, msg))
        }

type Topic internal (config, topic) = 
    let cp = ConfigurationRegistry.Resolve<ClientProvider>(config)
    let ns = cp.NamespaceClient
    let tc = cp.TopicClient(topic)
    member __.Name = topic
    member __.GetSubscription(affinity) : Subscription = new Subscription(config, topic, affinity)
    
    member __.EnqueueBatch<'T>(xs : ('T * string) []) = 
        async { 
            let! ys = xs
                      |> Array.map (fun (x, affinity) -> 
                             async { 
                                 let! bc = BlobCell.Init(config, topic, fun () -> x)
                                 let msg = new BrokeredMessage((bc :> IResource).Uri)
                                 msg.Properties.Add(AffinityPropertyName, affinity)
                                 return msg
                             })
                      |> Async.Parallel
            do! ofTask <| tc.SendBatchAsync(ys)
        }

    member __.EnqueueBatch<'T>(xs : 'T [], affinity : string) = 
        async { 
            let! ys = xs
                      |> Array.map (fun x -> 
                             async { 
                                 let! bc = BlobCell.Init(config, topic, fun () -> x)
                                 let msg = new BrokeredMessage((bc :> IResource).Uri)
                                 msg.Properties.Add(AffinityPropertyName, affinity)
                                 return msg
                             })
                      |> Async.Parallel
            do! ofTask <| tc.SendBatchAsync(ys)
        }
    
    member __.Enqueue<'T>(t : 'T, affinity : string) = 
        async { 
            let! bc = BlobCell.Init(config, topic, fun () -> t)
            let msg = new BrokeredMessage((bc :> IResource).Uri)
            msg.Properties.Add(AffinityPropertyName, affinity)
            do! ofTask <| tc.SendAsync(msg)
        }
    
    interface ISerializable with
        member x.GetObjectData(info : SerializationInfo, context : StreamingContext) : unit = 
            info.AddValue("topic", topic, typeof<string>)
            info.AddValue("config", config, typeof<ConfigurationId>)
    
    new(info : SerializationInfo, context : StreamingContext) = 
        let topic = info.GetValue("topic", typeof<string>) :?> string
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new Topic(config, topic)

    static member Init(config, name) = 
        async { 
            let ns = ConfigurationRegistry.Resolve<ClientProvider>(config).NamespaceClient
            if not <| ns.TopicExists(name) then 
                let td = new TopicDescription(name)
                td.EnableBatchedOperations <- true
                td.EnablePartitioning <- true
                td.DefaultMessageTimeToLive <- MaxTTL
                do! ofTask <| ns.CreateTopicAsync(td)
            return new Topic(config, name)
        }

/// Queue implementation.
type Queue internal (config : ConfigurationId, res : Uri) = 
    let queue = ConfigurationRegistry.Resolve<ClientProvider>(config).QueueClient(res.Queue)
    let ns = ConfigurationRegistry.Resolve<ClientProvider>(config).NamespaceClient
    member __.Length = ns.GetQueue(res.Queue).MessageCount
    
    member __.EnqueueBatch<'T>(xs : 'T []) = 
        async { 
            let! ys = xs
                      |> Array.map (fun x -> async { let! bc = BlobCell.Init(config, res.Queue, fun () -> x)
                                                     return new BrokeredMessage((bc :> IResource).Uri) })
                      |> Async.Parallel
            do! ofTask <| queue.SendBatchAsync(ys)
        }
    
    member __.Enqueue<'T>(t : 'T) = 
        async { 
            let! bc = BlobCell.Init(config, res.Queue, fun () -> t)
            let msg = new BrokeredMessage((bc :> IResource).Uri)
            do! ofTask <| queue.SendAsync(msg)
        }
    
    member __.TryDequeue<'T>() : Async<QueueMessage option> = 
        async { 
            let! msg = queue.ReceiveAsync(ServerWaitTime)
            if msg = null then return None
            else return Some(QueueMessage(config, msg))
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
    static member Init(config, container : string) = 
        async { 
            let res = Queue.GetUri container
            let ns = ConfigurationRegistry.Resolve<ClientProvider>(config).NamespaceClient
            let qd = new QueueDescription(res.Queue)
            qd.EnableBatchedOperations <- true
            qd.EnablePartitioning <- true
            qd.DefaultMessageTimeToLive <- MaxTTL
            qd.LockDuration <- MaxLockDuration
            let! exists = ns.QueueExistsAsync(res.Queue)
            if not exists then do! ofTask <| ns.CreateQueueAsync(qd)
            return new Queue(config, res)
        }