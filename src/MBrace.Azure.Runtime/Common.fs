module Nessos.MBrace.Azure.Runtime.Common

// Contains types used a table storage entities, service bus messages and blog objects.
open System
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging

type AzureConfig = 
    { StorageConnectionString : string
      ServiceBusConnectionString : string }

type IResource = 
    abstract Uri : Uri

[<AbstractClass; Sealed>]
type ClientProvider private () = 
    static let cfg = ref None
    static let acc = ref Unchecked.defaultof<CloudStorageAccount>
    
    static let check f = 
        lock cfg (fun () -> 
            if cfg.Value.IsNone then failwith "No active configuration found."
            else f())
    
    static member Activate(config : AzureConfig) = 
        let sa = CloudStorageAccount.Parse(config.StorageConnectionString)
        lock cfg (fun () -> 
            cfg := Some config
            acc := sa)
    
    static member ActiveConfiguration = check (fun _ -> cfg.Value.Value)
    static member TableClient = check (fun _ -> acc.Value.CreateCloudTableClient())
    static member BlobClient = check (fun _ -> acc.Value.CreateCloudBlobClient())
    static member NamespaceClient = 
        check (fun _ -> NamespaceManager.CreateFromConnectionString(cfg.Value.Value.ServiceBusConnectionString))
    static member QueueClient(queue : string) = 
        check (fun _ -> QueueClient.CreateFromConnectionString(cfg.Value.Value.ServiceBusConnectionString, queue))

//
// Table storage entities
//
// Parameterless public ctor is needed.

type CounterEntity(name : string, value : int) = 
    inherit TableEntity(name, String.Empty)
    member val Value = value with get, set
    new () = new CounterEntity(null, 0)

type LatchEntity(name : string, value : int, size : int) = 
    inherit TableEntity(name, String.Empty)
    member val Value = value with get, set
    member val Size = size with get, set
    new () = new LatchEntity(null, -1, -1)

type LightCellEntity(name : string, uri : Uri) =
    inherit TableEntity(name, uri.ToString())
    member val Uri = uri with get, set
    new () = LightCellEntity(null, null)

type ResultAggregatorEntity(name : string, index : int, bloburi : string) = 
    inherit TableEntity(name, string index)
    member val Index = index with get, set
    member val BlobCellUri = bloburi with get, set
    new () = new ResultAggregatorEntity(null, -1, null)

module Table =
    let insert<'T when 'T :> ITableEntity> table (e : 'T) : Async<unit> = 
        async { 
            let t = ClientProvider.TableClient.GetTableReference(table)
            let! _ = t.CreateIfNotExistsAsync()
            let! e = t.ExecuteAsync(TableOperation.Insert(e))
            return ()
        }
    
    let read<'T when 'T :> ITableEntity> table pk rk : Async<'T> = 
        async { 
            let t = ClientProvider.TableClient.GetTableReference(table)
            let! e = t.ExecuteAsync(TableOperation.Retrieve<'T>(pk, rk))
            return e.Result :?> 'T
        }
    
    let readBatch<'T when 'T : (new : unit -> 'T) and 'T :> ITableEntity> table pk : Async<seq<'T>> = 
        async {  
            let t = ClientProvider.TableClient.GetTableReference(table)
            let q = TableQuery<'T>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk))
            return t.ExecuteQuery<'T>(q)
        }
    
    let merge<'T when 'T :> ITableEntity> table (e : 'T) : Async<'T> = 
        async { 
            let t = ClientProvider.TableClient.GetTableReference(table)
            let! U = t.ExecuteAsync(TableOperation.Merge(e))
            return U.Result :?> 'T
        }
    
    let replace<'T when 'T :> ITableEntity> table (e : 'T) : Async<'T> = 
        async { 
            let t = ClientProvider.TableClient.GetTableReference(table)
            let! U = t.ExecuteAsync(TableOperation.Replace(e))
            return U.Result :?> 'T
        }