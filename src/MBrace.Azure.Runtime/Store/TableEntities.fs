namespace MBrace.Azure.Store.TableEntities

open System
open System.IO
open System.Threading.Tasks

open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table

open MBrace.Azure.Runtime.Utilities
open MBrace.Azure.Store

module internal TableEntityConfig =

    let PayloadSizePerProperty = 64 * 1024
    let NumberOfProperties = 15
    let MaxPayloadSize = NumberOfProperties * PayloadSizePerProperty

/// A lightweight object for low latency communication with the azure storage.
/// Lightweight : payload size up to 15 * 64K = 960K.
/// See 'http://www.windowsazure.com/en-us/develop/net/how-to-guides/table-services/'
/// WARNING : See the above link for any restrictions such as having a parameterless ctor,
/// and public properties.
[<AllowNullLiteral>]
type FatEntity (partitionKey : string, rowKey : string, blob : byte []) =
    inherit TableEntity(partitionKey, rowKey)

    let chunks = 
        if blob = null then null
        elif blob.Length > TableEntityConfig.MaxPayloadSize then
            invalidOp <| sprintf "Blob payload exceeds limit of %d bytes." TableEntityConfig.MaxPayloadSize
        else 
            Array.chunksOf TableEntityConfig.PayloadSizePerProperty blob

    let item i = 
        let i = i - 1
        if chunks = null then null
        elif i >= chunks.Length then Array.empty
        else
            chunks.[i]

    /// Max size 64KB
    member val Item01 = item 1  with get, set
    member val Item02 = item 2  with get, set
    member val Item03 = item 3  with get, set
    member val Item04 = item 4  with get, set
    member val Item05 = item 5  with get, set
    member val Item06 = item 6  with get, set
    member val Item07 = item 7  with get, set
    member val Item08 = item 8  with get, set
    member val Item09 = item 9  with get, set
    member val Item10 = item 10 with get, set
    member val Item11 = item 11 with get, set
    member val Item12 = item 12 with get, set
    member val Item13 = item 13 with get, set
    member val Item14 = item 14 with get, set
    member val Item15 = item 15 with get, set

    member this.GetPayload () =
        if this.Item01 = null then null
        else
            [| this.Item01; this.Item02; this.Item03; this.Item04; this.Item05; this.Item06; this.Item07; this.Item08; this.Item09; 
               this.Item10; this.Item11; this.Item12; this.Item13; this.Item14; this.Item15; |]
            |> Array.concat
        
    new () = FatEntity (null, null, null)


module internal Table =
    let private checkExn code (e : exn) =
        match e with
        | :? StorageException as e -> e.RequestInformation.HttpStatusCode = code
        | :? AggregateException as e ->
            let e = e.InnerException
            e :? StorageException && (e :?> StorageException).RequestInformation.HttpStatusCode = code
        | _ -> false

    
    let PreconditionFailed (e : exn) = checkExn 412 e
    let Conflict (e : exn) = checkExn 409 e
    let NotFound (e : exn) = checkExn 404 e

    let getClient (account : CloudStorageAccount) =
        account.CreateCloudTableClient()

    let getRandomName () =
        // See http://blogs.msdn.com/b/jmstall/archive/2014/06/12/azure-storage-naming-rules.aspx
        let alpha = [|'a'..'z'|]
        let alphaNumeric = Array.append alpha [|'0'..'9'|]
        let maxLen = 63
        let randOf =
            let rand = new Random(int DateTime.Now.Ticks)
            fun (x : char []) -> x.[rand.Next(0, x.Length)]

        let name = 
            [| yield randOf alpha
               for _i = 1 to maxLen-1 do yield randOf alphaNumeric |]
        new String(name)

    let private exec<'U> (client : CloudTableClient) table op : Async<obj> = 
        async {
            let t = client.GetTableReference(table)
            let! _ = t.CreateIfNotExistsAsync()
            let! (e : TableResult) = t.ExecuteAsync(op)
            return e.Result 
        }

    let insert<'T when 'T :> ITableEntity> (client : CloudTableClient) table (e : 'T) : Async<unit> = 
        TableOperation.Insert(e) |> exec client table |> Async.Ignore

    let batch (client : CloudTableClient) table (operations : TableBatchOperation) = 
        async {
            let jobs = new ResizeArray<Async<unit>>()
            let batch = ref <| new TableBatchOperation()
            let mkHandle batch = Async.StartChild <| async {
                let t = client.GetTableReference(table)
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

    let insertBatch<'T when 'T :> ITableEntity> (client : CloudTableClient) table (es : seq<'T>) : Async<unit> =
        let b = new TableBatchOperation()
        es |> Seq.iter (fun e -> b.Add(TableOperation.Insert(e)))
        batch client table b

    let mergeBatch<'T when 'T :> ITableEntity> (client : CloudTableClient) table (es : seq<'T>) : Async<unit> =
        let b = new TableBatchOperation()
        es |> Seq.iter (fun e -> b.Add(TableOperation.Merge(e)))
        batch client table b

    let deleteBatch<'T when 'T :> ITableEntity> (client : CloudTableClient) table (es : seq<'T>) : Async<unit> =
        let b = new TableBatchOperation()
        es |> Seq.iter (fun e -> b.Add(TableOperation.Delete(e)))
        batch client table b

    let insertOrReplace<'T when 'T :> ITableEntity> (client : CloudTableClient) table (e : 'T) : Async<unit> = 
        TableOperation.InsertOrReplace(e) |> exec client table |> Async.Ignore

    let insertOrMerge<'T when 'T :> ITableEntity> (client : CloudTableClient) table (e : 'T) : Async<unit> = 
        TableOperation.InsertOrMerge(e) |> exec client table |> Async.Ignore
    
    let queryDynamic (client : CloudTableClient) table pk : Async<DynamicTableEntity []> =
        async {  
            let t = client.GetTableReference(table)
            let q = TableQuery<DynamicTableEntity>()
                        .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk))
                        .Select([|"RowKey"|])
            return t.ExecuteQuery(q)
                   |> Seq.toArray
        }

    let read<'T when 'T :> ITableEntity> (client : CloudTableClient) table pk rk : Async<'T> = 
        async { 
            let t = client.GetTableReference(table)
            let! (e : TableResult) = t.ExecuteAsync(TableOperation.Retrieve<'T>(pk, rk))
            return e.Result :?> 'T
        }
    
    let readAll<'T when 'T : (new : unit -> 'T) and 'T :> ITableEntity> (client : CloudTableClient) table =
        async {
            let table = client.GetTableReference(table)
            let rec loop token = seq {
                let result = table.ExecuteQuerySegmented(new TableQuery<'T>(), token)
                yield! result.Results
                let token = result.ContinuationToken
                if token <> null then yield! loop token
            }
            return loop null
        }

    let query<'T when 'T : (new : unit -> 'T) and 'T :> ITableEntity> (client : CloudTableClient) table query =
        async {
            let t = client.GetTableReference(table)
            return t.ExecuteQuery<'T>(query)
                   |> Seq.toArray
        }

    let queryPK<'T when 'T : (new : unit -> 'T) and 'T :> ITableEntity> (client : CloudTableClient) table pk : Async<'T []> = 
        async {  
            let t = client.GetTableReference(table)
            let q = TableQuery<'T>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk))
            return t.ExecuteQuery<'T>(q)
                   |> Seq.toArray
        }
    
    let merge<'T when 'T :> ITableEntity> (client : CloudTableClient) table (e : 'T) : Async<'T> = 
        TableOperation.Merge(e) |> exec client table |> Async.Cast
    
    let replace<'T when 'T :> ITableEntity> (client : CloudTableClient) table (e : 'T) : Async<'T> = 
        TableOperation.Replace(e) |> exec client table |> Async.Cast

    let delete<'T when 'T :> ITableEntity> (client : CloudTableClient) table (e : 'T) : Async<unit> =
        TableOperation.Delete(e) |> exec client table |> Async.Ignore

    let transact<'T when 'T :> ITableEntity> (client : CloudTableClient) table pk rk (f : 'T -> unit) : Async<'T> =
        async {
            let rec transact e = async { 
                f e
                let! result = Async.Catch <| merge<'T> client table e
                match result with
                | Choice1Of2 r -> return r
                | Choice2Of2 ex when PreconditionFailed ex -> 
                    let! e = read<'T> client table pk rk
                    return! transact e
                | Choice2Of2 ex -> return raise ex
            }
            let! e = read<'T> client table pk rk
            return! transact e
        }

    let transact2<'T when 'T :> ITableEntity> (client : CloudTableClient) table pk rk (f : 'T -> 'T) : Async<'T> =
        async {
            let rec transact e = async { 
                let e' = f e
                let! result = Async.Catch <| merge<'T> client table e'
                match result with
                | Choice1Of2 r -> return r
                | Choice2Of2 ex when PreconditionFailed ex -> 
                    let! e = read<'T> client table pk rk
                    return! transact e
                | Choice2Of2 ex -> return raise ex
            }
            let! e = read<'T> (client : CloudTableClient) table pk rk
            return! transact e
        }