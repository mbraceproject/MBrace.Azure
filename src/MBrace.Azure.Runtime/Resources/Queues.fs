namespace Nessos.MBrace.Azure.Runtime.Resources

open System
open System.Runtime.Serialization
open Microsoft.ServiceBus.Messaging
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Common
open Nessos.MBrace.Azure.Runtime.Config


/// Queue implementation.
type Queue<'T> internal (res : Uri) = 
    let queue = ClientProvider.QueueClient(res.Queue)
    let ns = ClientProvider.NamespaceClient

    do 
        let rec f _ = async {
            let! _ = queue.PeekAsync() 
            return! f ()
        }
        Async.Start(f ())

    member __.Length = ns.GetQueue(res.Queue).MessageCount
    
    member __.Enqueue(t : 'T) = 
        async { 
            let r = BlobCell.GetUri(res.Queue)
            let! bc = BlobCell.Init(r, fun () -> t)
            let msg = new BrokeredMessage((bc :> IResource).Uri)
            do! ofTask <| queue.SendAsync(msg)
        } |> Async.RunSynchronously
    
    member __.TryDequeue() : Async<'T option> = 
        async { 
            let! msg = queue.ReceiveAsync()
            if msg = null then return None
            else 
                let p = msg.GetBody<Uri>()
                let t = BlobCell.Get<'T>(p)
                do! ofTask <| msg.CompleteAsync()
                let! v = t.GetValue()
                return Some v
        } 

    interface IResource with
        member x.Dispose(): unit = 
            ns.DeleteQueue(res.Queue)

         member __.Uri = res
    
    static member Init<'T>(res : Uri) = 
        async { 
            let ns = ClientProvider.NamespaceClient
            let qd = new QueueDescription(res.Queue)
            qd.DefaultMessageTimeToLive <- TimeSpan.MaxValue
            let! exists = ns.QueueExistsAsync(res.Queue)
            if not exists then do! ofTask <| ns.CreateQueueAsync(qd)
            return new Queue<'T>(res)
        }
    
    static member Get<'T>(res) = new Queue<'T>(res)

    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, context: StreamingContext): unit = 
            info.AddValue("uri", res, typeof<Uri>)

    new(info: SerializationInfo, context: StreamingContext) =
        let res = info.GetValue("uri", typeof<Uri>) :?> Uri
        new Queue<'T>(res)

type Queue =
    static member GetUri(container) = uri "queue:%s" container