module Nessos.MBrace.Azure.Runtime.Resources

// Contains types used a table storage entities, service bus messages and blog objects.
open System
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging
open Nessos.MBrace.Azure.Runtime.Common

/// Named latch implementation.
type Latch private (res : Uri) = 
    member __.Value = 
        let e = Table.read<LatchEntity> res.Table res.PartitionKey "" 
                |> Async.RunSynchronously
        e.Value
    
    member __.Increment() = 
        async { 
            let rec update () = 
                async { 
                    let! e = Table.read<LatchEntity> res.Table res.PartitionKey "" 
                    e.Value <- e.Value + 1
                    let r = ref None
                    try 
                        let! result = Table.merge res.Table e
                        r := Some result
                    with :? StorageException as se when se.RequestInformation.HttpStatusCode = 412 -> r := None
                    match r.Value with
                    | None -> return! update()
                    | Some r -> return r.Value
                }
            return! update()
        }
    
    static member Init(res : Uri, ?value : int) = 
        async {
            let value = defaultArg value 0
            let e = new LatchEntity(res.PartitionKey, value)
            do! Table.insert res.Table e
            return new Latch(res)
        }
    
    static member Get(res : Uri) = new Latch(res)
    
    interface IResource with
        member __.Uri = res
    
    static member GetUri(container, id) = uri "latch:%s/%s" container id
    static member GetUri(container) = Latch.GetUri(container, guid())


/// Read-only blob.   
type BlobCell private (res : Uri) = 
    let container = ClientProvider.BlobClient.GetContainerReference(res.Container)
    
    member __.GetValue<'T>() = 
        async {
            use! s = container.GetBlockBlobReference(res.File).OpenReadAsync()
            return Config.serializer.Deserialize<'T>(s)
        }
    
    interface IResource with
        member __.Uri = res
    
    static member Init(res : Uri, f : unit -> 'T) = 
        async {
            let c = ClientProvider.BlobClient.GetContainerReference(res.Container)
            let! _ = c.CreateIfNotExistsAsync()
            use! s = c.GetBlockBlobReference(res.File).OpenWriteAsync()
            Config.serializer.Serialize<'T>(s, f())
            return new BlobCell(res)
        }
    
    static member Get(res : Uri) = new BlobCell(res)
    static member GetUri(container, id) = uri "blobcell:%s/%s" container id
    static member GetUri(container) = BlobCell.GetUri(container, guid())

type TableCell private (res : Uri) = 
    member __.GetValue() : Async<'T> =
        async { 
            let! e = Table.read<LightCellEntity> res.Table res.PartitionKey ""
            let bc = BlobCell.Get(e.Uri)
            return! bc.GetValue<'T>()
        }

    static member Init(res : Uri, f : unit -> 'T) = 
        async {
            let res' = BlobCell.GetUri(res.Container)
            let! bc = BlobCell.Init(res', f)
            let e = new LightCellEntity(res.PartitionKey, res)
            do! Table.insert res.Table e
            return new TableCell(res)
        }
    
    static member Get(res : Uri) = new TableCell(res)
    
    interface IResource with
        member __.Uri = res
    
    static member GetUri(container, id) = uri "tablecell:%s/%s" container id
    static member GetUri(container) = TableCell.GetUri(container, guid())



/// Queue implementation.
type Queue private (res : Uri) = 
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
    
    static member Get(res) = new Queue(res)
    
    static member Init(res : Uri) = async {
            let ns = ClientProvider.NamespaceClient
            let qd = new QueueDescription(res.Queue)
            qd.DefaultMessageTimeToLive <- TimeSpan.MaxValue
            let! exists = ns.QueueExistsAsync(res.Queue)
            if not exists then 
                do! ofTask <| ns.CreateQueueAsync(qd) 
            return new Queue(res)
        }
    
    interface IResource with
        member __.Uri = res
    
    static member GetUri(container) = uri "queue:%s" container

type ResultCell private (res : Uri) = 
    let queue = Queue.Get(Queue.GetUri(res.Queue))
    member __.SetResult(result : 'T) = queue.Enqueue(result)
    member __.TryGetResult() = queue.TryDequeue()
    
    member __.AwaitResult() = 
        async {
            let! r = __.TryGetResult()
            match r with
            | None -> return! __.AwaitResult()
            | Some r -> r
        }
    
    interface IResource with
        member __.Uri = res
    
    static member GetUri(container) = uri "resultcell:%s/" container
    static member Get(res : Uri) = new ResultCell(res)
    static member Init(res : Uri) = 
        async {
            let! q = Queue.Init(res)
            return new ResultCell(res)
        }

type ResultAggregator private (res : Uri) = 
    
    member __.SetResult(index : int, value : 'T) : Async<bool> = 
        async {
            let e = new ResultAggregatorEntity(res.PartitionKey, string index, null, ETag = "*")
            let bcu = BlobCell.GetUri(res.Container)
            let! bc = BlobCell.Init(bcu, fun () -> value)
            e.BlobCellUri <- bcu.ToString()
            let! u = Table.merge res.Table e
            let l = Latch.Get(Latch.GetUri(res.Table, res.PartitionKey))
            let! curr = l.Increment() 
            return curr = int u.Size
        }
          
    member __.ToArray () : Async<'T []> = failwithf "Not implemented"
    
    member __.Completed () : Async<bool> = failwith "Not implemented"

    interface IResource with
        member __.Uri = res
    
    static member Get(res : Uri) = new ResultAggregator(res)
    static member GetUri(container, id) = uri "aggregator:%s/%s" container id
    static member GetUri(container) = Latch.GetUri(container, guid())

    static member Init(res : Uri, size : int) = 
        async {
            let! l = Latch.Init(Latch.GetUri(res.Table, res.PartitionKey), 0)
            for i = 0 to size-1 do
                let e = new ResultAggregatorEntity(res.PartitionKey, string i, string size)
                do! Table.insert res.Table e
            return new ResultAggregator(res)
        }
