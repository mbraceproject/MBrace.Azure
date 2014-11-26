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

//    member __.ReceiveBatch(count : int) = 
//        async {
//            let! xs = queue.ReceiveBatchAsync(count)
//            let xs = Seq.toArray xs
//            let ys = Array.zeroCreate<'T> xs.Length
//            for i = 0 to xs.Length - 1 do
//                let! v = messageToValue xs.[i]
//                ys.[i] <- v
//            return ys
//        }

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