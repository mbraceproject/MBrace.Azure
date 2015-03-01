namespace MBrace.Azure.Runtime.Resources

open MBrace.Runtime.Utils
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common
open MBrace.Azure.Runtime.Resources
open System
open System.Runtime.Serialization
open MBrace.Continuation
open MBrace
open System.Threading
open MBrace.Azure
open Microsoft.WindowsAzure.Storage.Table

/// Result value
type Result<'T> =
    | Completed of 'T
    | Exception of ExceptionDispatchInfo
    | Cancelled of OperationCanceledException
with
    member inline r.Value =
        match r with
        | Completed t -> t
        | Exception edi -> ExceptionDispatchInfo.raise true edi
        | Cancelled c -> ExceptionDispatchInfo.raiseWithCurrentStackTrace true c

type ResultCell<'T> internal (config : ConfigurationId, pk, rk) as self = 
    let table = config.RuntimeTable
    let localCell = lazy CacheAtom.Create((fun () -> self.TryGetResult() |> Async.RunSync), intervalMilliseconds = 200)

    member this.Path = sprintf "%s/%s" pk rk

    interface ICloudTask<'T> with
        member c.Id = pk

        member c.AwaitResult(?timeout:int) = cloud {
            let! r = Cloud.OfAsync <| Async.WithTimeout(c.AwaitResult(), defaultArg timeout Timeout.Infinite)
            return r.Value
        }

        member c.TryGetResult() = cloud {
            let! r = Cloud.OfAsync <| c.TryGetResult()
            return r |> Option.map (fun r -> r.Value)
        }

        member c.IsCompleted = 
            match localCell.Value.Value with
            | Some(Completed _) -> true
            | _ -> false

        member c.IsFaulted =
            match localCell.Value.Value with
            | Some(Exception _) -> true
            | _ -> false

        member c.IsCanceled =
            match localCell.Value.Value with
            | Some(Cancelled _) -> true
            | _ -> false

        member c.Status =
            match localCell.Value.Value with
            | Some (Completed _) -> Tasks.TaskStatus.RanToCompletion
            | Some (Exception _) -> Tasks.TaskStatus.Faulted
            | Some (Cancelled _) -> Tasks.TaskStatus.Canceled
            | None -> Tasks.TaskStatus.Running

        member c.Result = 
            async {
                let! r = c.AwaitResult()
                return r.Value
            } |> Async.RunSync
        

    member __.SetResult(result : Result<'T>) : Async<unit> =
        async {
            let! bc = Blob.CreateIfNotExists(config, pk, guid(), fun () -> result)
            let uri = bc.Path
            let e = new BlobReferenceEntity(pk, rk, uri.ToString(), ETag = "*")
            let! _ = Table.merge config table e
            return ()
        }

    member __.TryGetResult() : Async<Result<'T> option> = 
        async {
            let! e = Table.read<BlobReferenceEntity> config table pk rk
            if String.IsNullOrEmpty e.Uri then return None
            else
                let bc = Blob.FromPath(config, e.Uri)
                let! v = bc.GetValue()
                return Some v
        }
    
    member __.AwaitResult() : Async<Result<'T>> = 
        async { 
            let! r = __.TryGetResult()
            match r with
            | None -> return! __.AwaitResult()
            | Some r -> return r
        }
    
    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, _ : StreamingContext): unit = 
            info.AddValue("pk", pk, typeof<string>)
            info.AddValue("rk", rk, typeof<string>)
            info.AddValue("config", config, typeof<ConfigurationId>)

    new(info: SerializationInfo, _ : StreamingContext) =
        let pk = info.GetValue("pk", typeof<string>) :?> string
        let rk = info.GetValue("rk", typeof<string>) :?> string
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new ResultCell<'T>(config, pk, rk)

    static member FromPath(config : ConfigurationId, path : string) = 
        let pkrk = path.Split('/')
        new ResultCell<'T>(config, pkrk.[0], pkrk.[1])
    static member Create(config, pid) : Async<ResultCell<'T>> = 
        ResultCell.Create(config, guid(), pid)
    static member Create(config, id, pid) : Async<ResultCell<'T>> = 
        async { 
            let e = new BlobReferenceEntity(pid, id, null, EntityType = "RESULT")
            do! Table.insert<BlobReferenceEntity> config config.RuntimeTable e
            return new ResultCell<'T>(config, pid, id)
        }


type ResultAggregator<'T> internal (config : ConfigurationId, pk, rk, size) = 
    static let mkRowKey name i = sprintf "%s:%010d" name i
    
    let table = config.RuntimeTable

    let getEntities () = async {
        let pkFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk)
        let lower = mkRowKey rk 0
        let upper = mkRowKey rk size
        let rkFilter = 
            TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, upper),
                TableOperators.And,
                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, lower))

        let query = TableQuery<BlobReferenceEntity>().Where(TableQuery.CombineFilters(pkFilter, TableOperators.And, rkFilter))

        let! xs = Table.query<BlobReferenceEntity> config table query
        return xs
    }

    let completed () =
        async {
            let! xs = getEntities()
            return xs |> Seq.forall (fun e -> not <| String.IsNullOrEmpty(e.Uri))
        }

    member __.SetResult(index : int, value : 'T) : Async<bool> = 
        async { 
            let e = new BlobReferenceEntity(pk, mkRowKey rk index, null, ETag = "*")
            let! bc = Blob.CreateIfNotExists(config, pk, guid(), fun () -> value)
            e.Uri <- bc.Path
            let! _ = Table.merge config table e
            return! completed()
        }
    
    member __.Complete = Async.RunSync(completed())
    
    member __.ToArray() : Async<'T []> = 
        async { 
            if not __.Complete then 
                return! Async.Raise <| new InvalidOperationException("Result aggregator incomplete.")
            else
                let! xs = getEntities()
                let bs = 
                    xs
                    |> Seq.sortBy (fun x -> x.RowKey)
                    |> Seq.map (fun x -> x.Uri)
                    |> Seq.map (fun x -> Blob<_>.FromPath(config, x))
                    |> Seq.toArray

                let re = Array.zeroCreate<'T> bs.Length
                let i = ref 0
                for b in bs do
                    let! v = b.GetValue()
                    re.[!i] <- v
                    incr i
                return re
        }
   
    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, _: StreamingContext): unit = 
            info.AddValue("pk", pk, typeof<string>)
            info.AddValue("rk", rk, typeof<string>)
            info.AddValue("size", size, typeof<int>)
            info.AddValue("config", config, typeof<ConfigurationId>)

    new(info: SerializationInfo, _: StreamingContext) =
        let pk = info.GetValue("pk", typeof<string>) :?> string
        let rk = info.GetValue("rk", typeof<string>) :?> string
        let size = info.GetValue("size", typeof<int>) :?> int
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new ResultAggregator<'T>(config, pk, rk, size)

    static member Create<'T>(config, size, pid) = 
        async { 
            let name = guid()
            let entities = seq {
                for i = 0 to size - 1 do
                    let name = mkRowKey name i
                    yield new BlobReferenceEntity(pid, name, String.Empty, EntityType = "AGGR")
            }
            do! Table.insertBatch config config.RuntimeTable entities
            return new ResultAggregator<'T>(config, pid, name, size)
        }