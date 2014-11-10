module Nessos.MBrace.Azure.Runtime.Common

// Contains types used a table storage entities, service bus messages and blog objects.

open System
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging
open System.IO

type AzureConfig =
    {
        StorageConnectionString : string
        ServiceBusConnectionString : string
    }

type AzureRef =
    {
        Container : string
        Id : string
    }

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
type Latch private (cp : ClientProvider, path : AzureRef) =
    let table = cp.TableClient.GetTableReference(path.Container)
    let result = table.Execute(TableOperation.Retrieve<LatchEntity>(path.Id, String.Empty))
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
        
    static member Init(cp : ClientProvider, path : AzureRef, ?value : int) =
        let value = defaultArg value 0
        let table = cp.TableClient.GetTableReference(path.Container)
        do table.CreateIfNotExists() |> ignore
        let e = new LatchEntity(path.Id, value)
        let result = table.Execute(TableOperation.Insert(e))
        new Latch(cp, path)

    static member Get(cp : ClientProvider, path : AzureRef) =
        new Latch(cp, path)
     
/// Read-only blob.   
type BlobCell<'T> private (cp : ClientProvider, path : AzureRef) =
    let container = cp.BlobClient.GetContainerReference(path.Container)

    member __.Value 
        with get () = 
            use s = container.GetBlockBlobReference(path.Id).OpenRead()
            Config.serializer.Deserialize<'T>(s)

    static member Init(cp : ClientProvider, path : AzureRef, f : unit -> 'T) =
        let c = cp.BlobClient.GetContainerReference(path.Container)
        c.CreateIfNotExists() |> ignore
        use s = c.GetBlockBlobReference(path.Id).OpenWrite()
        Config.serializer.Serialize<'T>(s, f ())
        new BlobCell<'T>(cp, path)

    static member Get(cp : ClientProvider, path : AzureRef) = new BlobCell<'T>(cp, path)

/// Queue implementation.
type Queue<'T> private (cp : ClientProvider, path : AzureRef) =
    let queue = cp.QueueClientFactory(path.Container)
    let ns = cp.NamespaceClient

    member __.Length = ns.GetQueue(path.Container).MessageCount

    member __.Enqueue (t : 'T) =
        let p = { Container = path.Container; Id = System.Guid.NewGuid().ToString("N") }
        let bc = BlobCell.Init(cp, p, fun () -> t)
        let msg = new BrokeredMessage(p)
        queue.Send(msg)

    member __.TryDequeue () : 'T option =
        let msg = queue.Receive()
        if msg = null then None
        else
            let p = msg.GetBody<AzureRef>()
            let t = BlobCell.Get(cp, p)
            msg.Complete()
            Some t.Value

    static member Get(cp, path) = new Queue<'T>(cp, path)

    static member Init(cp : ClientProvider, path : AzureRef) =
        let ns = cp.NamespaceClient
        let qd = new QueueDescription(path.Container)
        qd.DefaultMessageTimeToLive <- TimeSpan.MaxValue
        if not <| ns.QueueExists(path.Container) then
            ns.CreateQueue(qd) |> ignore
        new Queue<'T>(cp, path)

type ResultCell<'T> private (queue : Queue<'T>) =

    member __.SetResult(result : 'T) = queue.Enqueue(result)

    member __.TryGetResult () = queue.TryDequeue()

    member __.AwaitResult () =
        match __.TryGetResult() with
        | None -> __.AwaitResult()
        | Some r -> r

    static member Get(cp : ClientProvider, path : AzureRef) =
        new ResultCell<'T>(Queue.Get(cp,path))

    static member Init(cp : ClientProvider, path : AzureRef) =
        let q = Queue.Init(cp, path)
        new ResultCell<'T>(q)

