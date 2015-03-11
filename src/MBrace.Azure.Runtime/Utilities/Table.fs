namespace MBrace.Azure.Runtime.Utilities

open System
open MBrace.Azure.Runtime
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table

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
            let t = ConfigurationRegistry.Resolve<StoreClientProvider>(config).TableClient.GetTableReference(table)
            let! _ = t.CreateIfNotExistsAsync()
            let! (e : TableResult) = t.ExecuteAsync(op)
            return e.Result 
        }

    let insert<'T when 'T :> ITableEntity> config table (e : 'T) : Async<unit> = 
        TableOperation.Insert(e) |> exec config table |> Async.Ignore

    let batch config table (operations : TableBatchOperation) = 
        async {
            let jobs = new ResizeArray<Async<unit>>()
            let batch = ref <| new TableBatchOperation()
            let mkHandle batch = Async.StartChild <| async {
                let t = ConfigurationRegistry.Resolve<StoreClientProvider>(config).TableClient.GetTableReference(table)
                let! _ = t.CreateIfNotExistsAsync()
                let! _ = t.ExecuteBatchAsync(batch)
                ()
            }
            for e in operations do
                batch.Value.Add(e)
                if batch.Value.Count = 100 then // Tables support up to 100 ops.
                    let! handle = mkHandle batch.Value
                    batch := new TableBatchOperation()
                    jobs.Add(handle)
            if batch.Value.Count > 0 then
                let! handle = mkHandle batch.Value
                jobs.Add(handle)

            do! Async.Parallel jobs
                |> Async.Ignore
        }

    let insertBatch<'T when 'T :> ITableEntity> config table (es : seq<'T>) : Async<unit> =
        let b = new TableBatchOperation()
        es |> Seq.iter (fun e -> b.Add(TableOperation.Insert(e)))
        batch config table b

    let mergeBatch<'T when 'T :> ITableEntity> config table (es : seq<'T>) : Async<unit> =
        let b = new TableBatchOperation()
        es |> Seq.iter (fun e -> b.Add(TableOperation.Merge(e)))
        batch config table b

    let deleteBatch<'T when 'T :> ITableEntity> config table (es : seq<'T>) : Async<unit> =
        let b = new TableBatchOperation()
        es |> Seq.iter (fun e -> b.Add(TableOperation.Delete(e)))
        batch config table b

    let insertOrReplace<'T when 'T :> ITableEntity> config table (e : 'T) : Async<unit> = 
        TableOperation.InsertOrReplace(e) |> exec config table |> Async.Ignore

    let insertOrMerge<'T when 'T :> ITableEntity> config table (e : 'T) : Async<unit> = 
        TableOperation.InsertOrMerge(e) |> exec config table |> Async.Ignore
    
    let queryDynamic config table pk : Async<DynamicTableEntity []> =
        async {  
            let t = ConfigurationRegistry.Resolve<StoreClientProvider>(config).TableClient.GetTableReference(table)
            let q = TableQuery<DynamicTableEntity>()
                        .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk))
                        .Select([|"RowKey"|])
            return t.ExecuteQuery(q)
                   |> Seq.toArray
        }

    let read<'T when 'T :> ITableEntity> config table pk rk : Async<'T> = 
        async { 
            let t = ConfigurationRegistry.Resolve<StoreClientProvider>(config).TableClient.GetTableReference(table)
            let! (e : TableResult) = t.ExecuteAsync(TableOperation.Retrieve<'T>(pk, rk))
            return e.Result :?> 'T
        }
    
    let query<'T when 'T : (new : unit -> 'T) and 'T :> ITableEntity> config table query =
        async {
            let t = ConfigurationRegistry.Resolve<StoreClientProvider>(config).TableClient.GetTableReference(table)
            return t.ExecuteQuery<'T>(query)
                   |> Seq.toArray
        }

    let queryPK<'T when 'T : (new : unit -> 'T) and 'T :> ITableEntity> config table pk : Async<'T []> = 
        async {  
            let t = ConfigurationRegistry.Resolve<StoreClientProvider>(config).TableClient.GetTableReference(table)
            let q = TableQuery<'T>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk))
            return t.ExecuteQuery<'T>(q)
                   |> Seq.toArray
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

    let transact2<'T when 'T :> ITableEntity> config table pk rk (f : 'T -> 'T) : Async<'T> =
        async {
            let rec transact e = async { 
                let e' = f e
                let! result = Async.Catch <| merge<'T> config table e'
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