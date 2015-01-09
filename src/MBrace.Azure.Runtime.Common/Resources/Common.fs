namespace MBrace.Azure.Runtime.Common

// Contains types used a table storage entities, service bus messages and blog objects.
open System
open System.Runtime.Serialization
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging
open MBrace.Azure.Runtime

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

type CancellationTokenSourceEntity(name : string) =
    inherit TableEntity(name, String.Empty)
    member val IsCancellationRequested = false with get, set
    new () = new CancellationTokenSourceEntity(null)

type CancellationTokenLinkEntity(parentUri : string, childUri : string) =
    inherit TableEntity(parentUri, childUri)
    new () = new CancellationTokenLinkEntity(null, null)


module Table =
    let PreconditionFailed (e : exn) =
        match e with
        | :? StorageException as e -> e.RequestInformation.HttpStatusCode = 412 
        | :? AggregateException as e ->
            let e = e.InnerException
            e :? StorageException && (e :?> StorageException).RequestInformation.HttpStatusCode = 412 
        | _ -> false

    let private exec<'U> config table op : Async<obj> = 
        async {
            let t = ConfigurationRegistry.Resolve<ClientProvider>(config).TableClient.GetTableReference(table)
            let! _ = t.CreateIfNotExistsAsync()
            let! (e : TableResult) = t.ExecuteAsync(op)
            return e.Result 
        }

    let insert<'T when 'T :> ITableEntity> config table (e : 'T) : Async<unit> = 
        TableOperation.Insert(e) |> exec config table |> Async.Ignore

    let insertBatch<'T when 'T :> ITableEntity> config table (e : seq<'T>) : Async<unit> =
        async {
            let batch = new TableBatchOperation()
            e |> Seq.iter batch.Insert
            let t = ConfigurationRegistry.Resolve<ClientProvider>(config).TableClient.GetTableReference(table)
            let! _ = t.CreateIfNotExistsAsync()
            let! es = t.ExecuteBatchAsync(batch)
            return ()
        }

    let insertOrReplace<'T when 'T :> ITableEntity> config table (e : 'T) : Async<unit> = 
        TableOperation.InsertOrReplace(e) |> exec config table |> Async.Ignore
    
    let read<'T when 'T :> ITableEntity> config table pk rk : Async<'T> = 
        async { 
            let t = ConfigurationRegistry.Resolve<ClientProvider>(config).TableClient.GetTableReference(table)
            let! (e : TableResult) = t.ExecuteAsync(TableOperation.Retrieve<'T>(pk, rk))
            return e.Result :?> 'T
        }
    
    let queryPK<'T when 'T : (new : unit -> 'T) and 'T :> ITableEntity> config table pk : Async<'T seq> = 
        async {  
            let t = ConfigurationRegistry.Resolve<ClientProvider>(config).TableClient.GetTableReference(table)
            let q = TableQuery<'T>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk))
            return t.ExecuteQuery<'T>(q)
        }
    
    let merge<'T when 'T :> ITableEntity> config table (e : 'T) : Async<'T> = 
        TableOperation.Merge(e) |> exec config table |> Async.Cast
    
    let replace<'T when 'T :> ITableEntity> config table (e : 'T) : Async<'T> = 
        TableOperation.Replace(e) |> exec config table |> Async.Cast

    let delete<'T when 'T :> ITableEntity> config table (e : 'T) : Async<unit> =
        TableOperation.Delete(e) |> exec config table |> Async.Ignore

    let transact<'T when 'T :> ITableEntity> config table pk rk (f : 'T -> unit) : Async<'T> =
        async {
            let rec transact e = async { 
                f e
                let r = ref None
                let! result = Async.Catch <| merge<'T> config table e
                match result with
                | Choice1Of2 r -> return r
                | Choice2Of2 ex when PreconditionFailed ex -> 
                    let! e = read<'T> config table pk rk
                    return! transact e
                | Choice2Of2 ex -> return raise ex
            }
            let! e = read<'T> config table pk rk
            return! transact e
        }