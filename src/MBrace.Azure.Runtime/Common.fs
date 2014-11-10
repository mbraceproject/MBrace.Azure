module Nessos.MBrace.Azure.Runtime.Common

// Contains types used a table storage entities, service bus messages and blog objects.

open System

open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging

type AzureConfig =
    {
        StorageConnectionString : string
        ServiceBusConnectionString : string
    }

type IResource =
    abstract Uri : Uri

[<AutoOpen>]
module UriUtils =
    let guid() = Guid.NewGuid().ToString("N")
    let uri fmt = Printf.ksprintf (fun s -> new Uri(s)) fmt
    let toContainerId(res : Uri) = 
        let s = res.Segments.[0] 
        s.Substring(0, s.Length - 1), res.Segments.[1]


type ClientProvider(config : AzureConfig) =
    let sa = CloudStorageAccount.Parse(config.StorageConnectionString)
    member __.TableClient = sa.CreateCloudTableClient()
    member __.BlobClient = sa.CreateCloudBlobClient()
    member __.NamespaceClient = NamespaceManager.CreateFromConnectionString(config.ServiceBusConnectionString)
    member __.QueueClientFactory(queue : string) = QueueClient.CreateFromConnectionString(config.ServiceBusConnectionString, queue)

// Parameterless public ctor is needed.
type LatchEntity(name : string, value : int) =
    inherit TableEntity(name, String.Empty)
    member val Value = value with get, set
    new () = new LatchEntity (null, 0)     

/// Named latch implementation.
type Latch private (cp : ClientProvider, res : Uri) =
    let table, id = toContainerId res
    let table = cp.TableClient.GetTableReference(table)
    let result = table.Execute(TableOperation.Retrieve<LatchEntity>(id, String.Empty))
    let entity = result.Result :?> LatchEntity
    
    let read () =
        let result = table.Execute(TableOperation.Retrieve<LatchEntity>(entity.PartitionKey, entity.RowKey))
        let e = result.Result :?> LatchEntity
        e

    let rec update () =
        let e = read ()
        e.Value <- e.Value + 1
        let r =        
            try
                let result = table.Execute(TableOperation.Merge(e))
                Some(result.Result :?> LatchEntity)
            with :? StorageException as se when se.RequestInformation.HttpStatusCode = 412 ->
                None
        match r with
        | None -> update ()
        | Some v -> v

    member __.Value with get () = let e = read () in e.Value

    member __.Increment () = update () |> ignore
        
    static member Init(cp : ClientProvider, res , ?value : int) =
        let value = defaultArg value 0
        let table, id = toContainerId res
        let table = cp.TableClient.GetTableReference(table)
        do table.CreateIfNotExists() |> ignore
        let e = new LatchEntity(id, value)
        let result = table.Execute(TableOperation.Insert(e))
        new Latch(cp, res)

    static member Get(cp : ClientProvider, path ) =
        new Latch(cp, path)
     
    interface IResource with
        member __.Uri = res

    static member GetUri(container, id) = uri "latch:%s/%s" container id
    static member GetUri(container) = Latch.GetUri(container, guid())

/// Read-only blob.   
type BlobCell private (cp : ClientProvider, res : Uri) =
    let container, id = toContainerId res
    let container = cp.BlobClient.GetContainerReference(container)

    member __.GetValue<'T>() =  
        use s = container.GetBlockBlobReference(id).OpenRead()
        Config.serializer.Deserialize<'T>(s)

    interface IResource with
        member __.Uri = res

    static member Init(cp : ClientProvider, res , f : unit -> 'T) =
        let container, id = toContainerId res
        let c = cp.BlobClient.GetContainerReference(container)
        c.CreateIfNotExists() |> ignore
        use s = c.GetBlockBlobReference(id).OpenWrite()
        Config.serializer.Serialize<'T>(s, f ())
        new BlobCell(cp, res)

    static member Get(cp : ClientProvider, res ) = new BlobCell(cp, res)

    static member GetUri(container, id) = uri "blobcell:%s/%s" container id
    static member GetUri(container) = BlobCell.GetUri(container, guid())

/// Queue implementation.
type Queue private (cp : ClientProvider, res : Uri) =
    let queueName = res.Segments.[0]
    let queue = cp.QueueClientFactory(queueName)
    let ns = cp.NamespaceClient

    member __.Length = ns.GetQueue(queueName).MessageCount

    member __.Enqueue (t : 'T) =
        let r = BlobCell.GetUri(queueName)
        let bc = BlobCell.Init(cp, r, fun () -> t)
        let msg = new BrokeredMessage(r)
        queue.Send(msg)

    member __.TryDequeue () : 'T option =
        let msg = queue.Receive()
        if msg = null then None
        else
            let p = msg.GetBody<Uri>()
            let t = BlobCell.Get(cp, p)
            msg.Complete()
            Some <| t.GetValue()

    static member Get(cp, res) = new Queue(cp, res)

    static member Init(cp : ClientProvider, res : Uri) =
        let ns = cp.NamespaceClient
        let container = res.Segments.[0]
        let qd = new QueueDescription(container)
        qd.DefaultMessageTimeToLive <- TimeSpan.MaxValue
        if not <| ns.QueueExists(container) then
            ns.CreateQueue(qd) |> ignore
        new Queue(cp, res)

    interface IResource with
        member __.Uri = res

    static member GetUri(container) = uri "queue:%s" container

type ResultCell private (cp : ClientProvider, res : Uri) =
    let queue = Queue.Get(cp, Queue.GetUri(res.Segments.[0]))

    member __.SetResult(result : 'T) = queue.Enqueue(result)

    member __.TryGetResult () = queue.TryDequeue()

    member __.AwaitResult () =
        match __.TryGetResult() with
        | None -> __.AwaitResult()
        | Some r -> r

    interface IResource with
        member __.Uri = res

    static member GetUri(container) = uri "resultcell:%s/" container

    static member Get(cp : ClientProvider, res : Uri) =
        new ResultCell(cp, res)

    static member Init(cp : ClientProvider, res : Uri) =
        let q = Queue.Init(cp, res)
        new ResultCell(cp, res)

