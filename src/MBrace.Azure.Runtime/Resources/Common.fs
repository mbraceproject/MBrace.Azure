namespace Nessos.MBrace.Azure.Runtime.Common

// Contains types used a table storage entities, service bus messages and blog objects.
open System
open System.Runtime.Serialization
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging
open Nessos.MBrace.Azure.Runtime


type IResource = 
    inherit ISerializable
    //inherit IDisposable
    abstract Uri : Uri 

//
// Table storage entities
//
// Parameterless public ctor is needed.

type CounterEntity(name : string, value : int) = 
    inherit TableEntity(name, String.Empty)
    member val Value = value with get, set
    new () = new CounterEntity(null, 0)

type LatchEntity(name : string, value : int, size : int) = 
    inherit CounterEntity(name, value)
    member val Size = size with get, set
    new () = new LatchEntity(null, -1, -1)

type LightCellEntity(name : string, uri : string) =
    inherit TableEntity(name, String.Empty)
    member val Uri = uri with get, set
    new () = LightCellEntity(null, null)

type ResultAggregatorEntity(name : string, index : int, bloburi : string) = 
    inherit TableEntity(name, string index)
    member val Index = index with get, set
    member val Uri = bloburi with get, set
    new () = new ResultAggregatorEntity(null, -1, null)

type CancellationTokenSourceEntity(name : string, link : string) =
    inherit TableEntity(name, String.Empty)
    member val IsCancellationRequested = false with get, set
    member val Link = link with get, set
    new () = new CancellationTokenSourceEntity(null, null)

module Table =
    open Nessos.MBrace.Azure.Runtime.Config

    let PreconditionFailed (e : exn) =
        match e with
        | :? AggregateException as e ->
            let e = e.InnerException
            e :? StorageException && (e :?> StorageException).RequestInformation.HttpStatusCode = 412 
        | _ -> false

    let private exec<'U> table op : Async<obj> = 
        async {
            let t = ClientProvider.TableClient.GetTableReference(table)
            let! _ = t.CreateIfNotExistsAsync()
            let! e = t.ExecuteAsync(op)
            return e.Result
        }

    let insert<'T when 'T :> ITableEntity> table (e : 'T) : Async<unit> = 
        TableOperation.Insert(e) |> exec table |> Async.Ignore

    let insertOrReplace<'T when 'T :> ITableEntity> table (e : 'T) : Async<unit> = 
        TableOperation.InsertOrReplace(e) |> exec table |> Async.Ignore
    
    let read<'T when 'T :> ITableEntity> table pk rk : Async<'T> = 
        async { 
            let t = ClientProvider.TableClient.GetTableReference(table)
            let! e = t.ExecuteAsync(TableOperation.Retrieve<'T>(pk, rk))
            return e.Result :?> 'T
        }
    
    let readBatch<'T when 'T : (new : unit -> 'T) and 'T :> ITableEntity> table pk : Async<'T seq> = 
        async {  
            let t = ClientProvider.TableClient.GetTableReference(table)
            let q = TableQuery<'T>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk))
            return t.ExecuteQuery<'T>(q)
        }
    
    let merge<'T when 'T :> ITableEntity> table (e : 'T) : Async<'T> = 
        TableOperation.Merge(e) |> exec table |> Async.Cast
    
    let replace<'T when 'T :> ITableEntity> table (e : 'T) : Async<'T> = 
        TableOperation.Replace(e) |> exec table |> Async.Cast

    let delete<'T when 'T :> ITableEntity> table (e : 'T) : Async<unit> =
        TableOperation.Delete(e) |> exec table |> Async.Ignore