namespace MBrace.Azure.Runtime.Common

// Contains types used a table storage entities, service bus messages and blog objects.
open System
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open MBrace.Azure.Runtime

//
// Table storage entities
//
// Parameterless public ctor is needed.
[<AutoOpen>]
module private Helper =
    let empty = Unchecked.defaultof<string>

[<AbstractClass>]
/// Base class for all process-bound runtime resources.
type RuntimeEntity (pid, rk, entity) =
    inherit TableEntity(pid, rk)
    member val EntityType = entity with get, set
    
type CounterEntity(pid, name : string, value : int) = 
    inherit RuntimeEntity(pid, name, "CNT")
    member val Value = value with get, set
    new () = new CounterEntity(empty, empty, 0)

type LatchEntity(pid, name : string, value : int, size : int) = 
    inherit CounterEntity(pid, name, value)
    member val Size = size with get, set
    new () = new LatchEntity(empty, empty, -1, -1)

type BlobReferenceEntity(pid, name : string, uri : string) =
    inherit RuntimeEntity(pid, name, "REF")
    member val Uri = uri with get, set
    new () = BlobReferenceEntity(empty, empty, empty)
    new(pid, name) = new BlobReferenceEntity(pid, name, empty)

// Stores around 10K CTS links.
type CancellationTokenSourceEntity(pid, id : string, links : (string * string) list) =
    inherit RuntimeEntity(pid, id, "CTS")

    let split =
        if box links <> null then
            let s = 
                links 
                |> Seq.map (fun (pk, rk) -> sprintf "%s/%s" pk rk)
                |> String.concat ";"
            let chunkSize = 64 * 1024 * 1024 // 64KB per property
            {0..chunkSize..s.Length-1}
            |> Seq.map(fun i -> s.Substring(i, Math.Min(chunkSize, s.Length-i)))
            |> Seq.toArray
        else null

    let check (a : string []) i =
        if a = null then null else if i >= a.Length then String.Empty else a.[i]

    member val IsCancellationRequested = false with get, set
    member val Metadata = empty with get, set

    member val Link0 = check split 0 with get, set
    member val Link1 = check split 1 with get, set
    member val Link2 = check split 2 with get, set
    member val Link3 = check split 3 with get, set
    member val Link4 = check split 4 with get, set
    member val Link5 = check split 5 with get, set
    member val Link6 = check split 6 with get, set
    member val Link7 = check split 7 with get, set
    member val Link8 = check split 8 with get, set
    member val Link9 = check split 9 with get, set

    member this.GetLinks () =
        String.Concat(
            this.Link0, this.Link1, this.Link2, this.Link3, this.Link4, 
            this.Link5, this.Link6, this.Link7, this.Link8, this.Link9).Split(';')
        |> Seq.filter(not << String.IsNullOrEmpty)
        |> Seq.map (fun pkrk -> let xs = pkrk.Split('/') in xs.[0], xs.[1])
        |> Seq.toList
        
    new () = new CancellationTokenSourceEntity(empty, empty, Unchecked.defaultof<_>)



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


    let batch<'T when 'T :> ITableEntity> (operation : 'T -> TableBatchOperation -> unit) config table (es : seq<'T>) : Async<unit> =
        async {
            let jobs = new ResizeArray<Async<unit>>()
            let batch = ref <| new TableBatchOperation()
            let mkHandle batch = Async.StartChild <| async {
                let t = ConfigurationRegistry.Resolve<ClientProvider>(config).TableClient.GetTableReference(table)
                let! _ = t.CreateIfNotExistsAsync()
                let! _ = t.ExecuteBatchAsync(batch)
                ()
            }
            for e in es do
                operation e batch.Value
                if batch.Value.Count = 100 then
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
        batch (fun e b -> b.Insert(e)) config table es

    let mergeBatch<'T when 'T :> ITableEntity> config table (es : seq<'T>) : Async<unit> =
        batch (fun e b -> b.Merge(e)) config table es

    let deleteBatch<'T when 'T :> ITableEntity> config table (es : seq<'T>) : Async<unit> =
        batch (fun e b -> b.Delete(e)) config table es

    let insertOrReplace<'T when 'T :> ITableEntity> config table (e : 'T) : Async<unit> = 
        TableOperation.InsertOrReplace(e) |> exec config table |> Async.Ignore
    
    let queryDynamic config table pk : Async<DynamicTableEntity seq> =
        async {  
            let t = ConfigurationRegistry.Resolve<ClientProvider>(config).TableClient.GetTableReference(table)
            let q = TableQuery<DynamicTableEntity>()
                        .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk))
                        .Select([|"RowKey"|])
            return t.ExecuteQuery(q)

        }

    let read<'T when 'T :> ITableEntity> config table pk rk : Async<'T> = 
        async { 
            let t = ConfigurationRegistry.Resolve<ClientProvider>(config).TableClient.GetTableReference(table)
            let! (e : TableResult) = t.ExecuteAsync(TableOperation.Retrieve<'T>(pk, rk))
            return e.Result :?> 'T
        }
    
    let query<'T when 'T : (new : unit -> 'T) and 'T :> ITableEntity> config table query =
        async {
            let t = ConfigurationRegistry.Resolve<ClientProvider>(config).TableClient.GetTableReference(table)
            return t.ExecuteQuery<'T>(query)
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