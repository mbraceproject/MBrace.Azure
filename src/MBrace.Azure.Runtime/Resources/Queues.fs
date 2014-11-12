module Nessos.MBrace.Azure.Runtime.Queues

open System
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging
open Nessos.MBrace.Azure.Runtime.Common
open Nessos.MBrace.Azure.Runtime.Counters
open Nessos.MBrace.Azure.Runtime.Cells

/// Queue implementation.
type Queue internal (res : Uri) = 
    let queue = ClientProvider.QueueClient(res.Queue)
    let ns = ClientProvider.NamespaceClient
    member __.Length = ns.GetQueue(res.Queue).MessageCount
    
    member __.Enqueue(t : 'T) = 
        async { 
            let r = BlobCell.GetUri(res.Queue)
            let! bc = BlobCell.Init(r, fun () -> t)
            let msg = new BrokeredMessage((bc :> IResource).Uri)
            do! ofTask <| queue.SendAsync(msg)
        }
    
    member __.TryDequeue() : Async<'T option> = 
        async { 
            let! msg = queue.ReceiveAsync()
            if msg = null then return None
            else 
                let p = msg.GetBody<Uri>()
                let t = BlobCell.Get(p)
                do! ofTask <| msg.CompleteAsync()
                let! v = t.GetValue()
                return Some v
        }

    interface IResource with member __.Uri = res
    
    static member Init(res : Uri) = 
        async { 
            let ns = ClientProvider.NamespaceClient
            let qd = new QueueDescription(res.Queue)
            qd.DefaultMessageTimeToLive <- TimeSpan.MaxValue
            let! exists = ns.QueueExistsAsync(res.Queue)
            if not exists then do! ofTask <| ns.CreateQueueAsync(qd)
            return new Queue(res)
        }
    
    static member Get(res) = new Queue(res)
    static member GetUri(container) = uri "queue:%s" container