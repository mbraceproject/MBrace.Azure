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
    let table, id = toContainerId res
    let table = ClientProvider.TableClient.GetTableReference(table)
    
    let read()  = 
        async { 
            let! result = table.ExecuteAsync(TableOperation.Retrieve<LatchEntity>(id, String.Empty))
            return result.Result :?> LatchEntity
        }
      
    member __.Value = let e = read() |> Async.RunSynchronously in e.Value
    
    member __.Increment() = 
        async { 
            let rec update() = 
                async { 
                    let! e = read()
                    e.Value <- e.Value + 1
                    let r = ref None
                    let! result = table.ExecuteAsync(TableOperation.Merge(e))
                    try 
                        r := Some(result.Result :?> LatchEntity)
                    with :? StorageException as se when se.RequestInformation.HttpStatusCode = 412 -> r := None
                    match r.Value with
                    | None -> return! update()
                    | Some _ -> return ()
                }
            return! update()
        }
    
    static member Init(res : Uri, ?value : int) = 
        async {
            let value = defaultArg value 0
            let table, id = toContainerId res
            let table = ClientProvider.TableClient.GetTableReference(table)
            let! _ = table.CreateIfNotExistsAsync()
            let e = new LatchEntity(id, value)
            let! result = table.ExecuteAsync(TableOperation.Insert(e))
            return new Latch(res)
        }
    
    static member Get(res : Uri) = new Latch(res)
    
    interface IResource with
        member __.Uri = res
    
    static member GetUri(container, id) = uri "latch:%s/%s" container id
    static member GetUri(container) = Latch.GetUri(container, guid())

/// Read-only blob.   
type BlobCell private (res : Uri) = 
    let container, id = toContainerId res
    let container = ClientProvider.BlobClient.GetContainerReference(container)
    
    member __.GetValue<'T>() = 
        async {
            use! s = container.GetBlockBlobReference(id).OpenReadAsync()
            return Config.serializer.Deserialize<'T>(s)
        }
    
    interface IResource with
        member __.Uri = res
    
    static member Init(res, f : unit -> 'T) = 
        async {
            let container, id = toContainerId res
            let c = ClientProvider.BlobClient.GetContainerReference(container)
            let! _ = c.CreateIfNotExistsAsync()
            use! s = c.GetBlockBlobReference(id).OpenWriteAsync()
            Config.serializer.Serialize<'T>(s, f())
            return new BlobCell(res)
        }
    
    static member Get(res : Uri) = new BlobCell(res)
    static member GetUri(container, id) = uri "blobcell:%s/%s" container id
    static member GetUri(container) = BlobCell.GetUri(container, guid())

/// Queue implementation.
type Queue private (res : Uri) = 
    let queueName = res.Segments.[0]
    let queue = ClientProvider.QueueClient(queueName)
    let ns = ClientProvider.NamespaceClient
    member __.Length = ns.GetQueue(queueName).MessageCount
    
    member __.Enqueue(t : 'T) = 
        async {
            let r = BlobCell.GetUri(queueName)
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
            let container = res.Segments.[0]
            let qd = new QueueDescription(container)
            qd.DefaultMessageTimeToLive <- TimeSpan.MaxValue
            let! exists = ns.QueueExistsAsync(container)
            if not exists then 
                do! ofTask <| ns.CreateQueueAsync(qd) 
            return new Queue(res)
        }
    
    interface IResource with
        member __.Uri = res
    
    static member GetUri(container) = uri "queue:%s" container

type ResultCell private (res : Uri) = 
    let queue = Queue.Get(Queue.GetUri(res.Segments.[0]))
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

//type ResultAggregator private (res : Uri) = 
//    let table, pk = toContainerId res
//    let table = ClientProvider.TableClient.GetTableReference(table)
//    
//    let read() = 
//        let result = table.Execute(TableOperation.Retrieve<LatchEntity>(pk, String.Empty))
//        let e = result.Result :?> LatchEntity
//        e
//    
//    let rec update() = 
//        let e = read()
//        e.Value <- e.Value + 1
//        let r = 
//            try 
//                let result = table.Execute(TableOperation.Merge(e))
//                Some(result.Result :?> LatchEntity)
//            with :? StorageException as se when se.RequestInformation.HttpStatusCode = 412 -> None
//        match r with
//        | None -> update()
//        | Some v -> v
//    
//    member __.SetResult(index : int, value : 'T) = 
//        let e = read()
//        e.Value
//    
//    member __.ToArray () : 'T [] = update() |> ignore
//    
//    static member Init(res : Uri, size : int) = 
//        let value = defaultArg value 0
//        let table, id = toContainerId res
//        let table = ClientProvider.TableClient.GetTableReference(table)
//        do table.CreateIfNotExists() |> ignore
//        let e = new LatchEntity(id, value)
//        let result = table.Execute(TableOperation.Insert(e))
//        new Latch(res)
//    
//    static member Get(res : Uri) = new Latch(res)
//    
//    interface IResource with
//        member __.Uri = res
//    
//    static member GetUri(container, id) = uri "aggregator:%s/%s" container id
//    static member GetUri(container) = Latch.GetUri(container, guid())
