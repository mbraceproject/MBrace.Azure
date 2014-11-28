namespace Nessos.MBrace.Azure.Runtime.Resources

open System
open System.Runtime.Serialization
open Microsoft.ServiceBus.Messaging
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Common

[<AutoOpenAttribute>]
module private Constants =
    let RenewLockInverval = 10000
    let MaxLockDuration   = 3 * RenewLockInverval
    let MaxTTL            = TimeSpan.MaxValue

type QueueMessage<'T> (config, msg : BrokeredMessage) =
    let completed = ref false
    let value = ref None

    member __.DeliveryCount = msg.DeliveryCount

    member __.CompleteAsync () = 
        async {
            completed := true
            do! ofTask <| msg.CompleteAsync()
        }

    member __.AbandonAsync () =         
        async {
            completed := true
            do! ofTask <| msg.AbandonAsync()
        }

    member __.RenewLoopAsync () = 
        let rec loop () =
            async {
                if !completed then return ()
                else
                    do! ofTask <| msg.RenewLockAsync()
                    do! Async.Sleep RenewLockInverval
                    return! loop ()
            }
        loop ()
    
    member __.GetPayloadAsync() : Async<'T> =
        async {
            match !value with
            | None ->
                let p = msg.GetBody<Uri>()
                let t = BlobCell.OfUri<'T>(config, p)
                let! v = t.GetValue()
                value := Some v
                return v
            | Some v -> return v
        }

type Subscription(config, topic, affinityId : string) =
    let ns = ConfigurationRegistry.Resolve<ClientProvider>(config).NamespaceClient
    //do
        //if not <| ns.SubscriptionExists(topic, affinityId) then
        //ns.CreateSubscription() |> ignore
    let sub = ns.GetSubscription(topic, affinityId)

    member __.EnqueueBatch<'T>(xs : 'T []) : Async<unit> =
        failwith "Not implemented"

    member __.Enqueue<'T>(t : 'T) : Async<unit> =
        failwith "Not implemented"

    member __.TryDequeue<'T>() : Async<QueueMessage<'T> option> = 
        failwith "Not implemented"


type Topic (config, topic) = 
    member __.GetSubscription(affinityId) : Subscription =
        new Subscription(config, topic, affinityId)

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
type Queue<'T> internal (config : ConfigurationId, res : Uri) = 
    let queue = ConfigurationRegistry.Resolve<ClientProvider>(config).QueueClient(res.Queue)
    let ns = ConfigurationRegistry.Resolve<ClientProvider>(config).NamespaceClient

    member __.Length = ns.GetQueue(res.Queue).MessageCount

    member __.EnqueueBatch(xs : 'T []) =
        async {
            let! ys = 
                xs
                |> Array.map (fun x -> 
                    async { 
                        let! bc = BlobCell.Init(config, res.Queue, fun () -> x)
                        return new BrokeredMessage((bc :> IResource).Uri) 
                    })
                |> Async.Parallel
            do! ofTask <| queue.SendBatchAsync(ys)
        }

    member __.Enqueue(t : 'T) = 
        async { 
            let! bc = BlobCell.Init(config, res.Queue, fun () -> t)
            let msg = new BrokeredMessage((bc :> IResource).Uri)
            do! ofTask <| queue.SendAsync(msg)
        } 
    
    member __.TryDequeue() : Async<QueueMessage<'T> option> = 
        async { 
            let! msg = queue.ReceiveAsync()
            if msg = null then return None
            else 
                return Some(QueueMessage<'T>(config, msg))
        } 

    interface IResource with
         member __.Uri = res

    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, context: StreamingContext): unit = 
            info.AddValue("uri", res, typeof<Uri>)
            info.AddValue("config", config, typeof<ConfigurationId>)

    new(info: SerializationInfo, context: StreamingContext) =
        let res = info.GetValue("uri", typeof<Uri>) :?> Uri
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new Queue<'T>(config, res)
    
    static member private GetUri(container) = uri "queue:%s" container
    static member Init<'T>(config, container : string) = 
        async { 
            let res = Queue<_>.GetUri container
            let ns = ConfigurationRegistry.Resolve<ClientProvider>(config).NamespaceClient
            let qd = new QueueDescription(res.Queue)
            qd.EnableBatchedOperations <- true
            qd.EnablePartitioning <- true
            qd.DefaultMessageTimeToLive <- MaxTTL
            qd.LockDuration <- TimeSpan.FromMilliseconds(float MaxLockDuration)
            let! exists = ns.QueueExistsAsync(res.Queue)
            if not exists then do! ofTask <| ns.CreateQueueAsync(qd)
            return new Queue<'T>(config, res)
        }