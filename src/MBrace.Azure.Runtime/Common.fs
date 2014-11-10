module Nessos.MBrace.Azure.Runtime.Common

// Contains types used a table storage entities, service bus messages and blog objects.

open System
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Microsoft.ServiceBus

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
        
