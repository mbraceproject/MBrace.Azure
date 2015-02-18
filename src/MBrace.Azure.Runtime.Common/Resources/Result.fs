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

type ResultCell<'T> internal (config, id, res : Uri) as self = 

    let mutable localCell : CacheAtom<Result<'T> option> option = None
    let getLocalCell() =
        match localCell with
        | Some c -> c
        | None ->
            lock self (fun () ->
                let cell = CacheAtom.Create((fun () -> self.TryGetResult() |> Async.RunSync), intervalMilliseconds = 200)
                localCell <- Some cell
                cell)

    interface ICloudTask<'T> with
        member c.Id = id

        member c.AwaitResult(?timeout:int) = cloud {
            let! r = Cloud.OfAsync <| Async.WithTimeout(c.AwaitResult(), defaultArg timeout Timeout.Infinite)
            return r.Value
        }

        member c.TryGetResult() = cloud {
            let! r = Cloud.OfAsync <| c.TryGetResult()
            return r |> Option.map (fun r -> r.Value)
        }

        member c.IsCompleted = 
            match getLocalCell().Value with
            | Some(Completed _) -> true
            | _ -> false

        member c.IsFaulted =
            match getLocalCell().Value with
            | Some(Exception _) -> true
            | _ -> false

        member c.IsCanceled =
            match getLocalCell().Value with
            | Some(Cancelled _) -> true
            | _ -> false

        member c.Status =
            match getLocalCell().Value with
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
            let! bc = BlobCell.CreateIfNotExists(config, res.Container, fun () -> result)
            let uri = bc.Uri
            let e = new LightCellEntity(res.PartitionWithScheme, uri.ToString(), ETag = "*")
            let! u = Table.merge config res.Table e
            return ()
        }

    member __.TryGetResult() : Async<Result<'T> option> = 
        async {
            let! e = Table.read<LightCellEntity> config res.Table res.PartitionWithScheme ""
            if e.Uri = null then return None
            else
                let bc = BlobCell.OfUri<Result<'T>>(config, new Uri(e.Uri))
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
    
    member __.Uri = res

    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, context: StreamingContext): unit = 
            info.AddValue("uri", res, typeof<Uri>)
            info.AddValue("id", id, typeof<string>)
            info.AddValue("config", config, typeof<ConfigurationId>)

    new(info: SerializationInfo, context: StreamingContext) =
        let res = info.GetValue("uri", typeof<Uri>) :?> Uri
        let id = info.GetValue("id", typeof<string>) :?> string
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new ResultCell<'T>(config, id, res)

    static member private GetUri(container, id) = uri "resultcell:%s/%s" container id
    //static member FromUri<'T>(config : ConfigurationId, uri) = new ResultCell<'T>(config, uri)
    static member Create<'T>(config, id, container : string) : Async<ResultCell<'T>> = 
        async { 
            let res = ResultCell<_>.GetUri(container, id)
            let e = new LightCellEntity(res.PartitionWithScheme, null)
            do! Table.insert<LightCellEntity> config res.Table e
            return new ResultCell<'T>(config, id, res)
        }


type ResultAggregator<'T> internal (config, res : Uri) = 
    
    let completed () =
        async {
            let! xs = Table.queryPK<ResultAggregatorEntity> config res.Table res.PartitionWithScheme
            return xs |> Seq.forall (fun e -> e.Uri <> String.Empty)
        }

    member __.SetResult(index : int, value : 'T) : Async<bool> = 
        async { 
            let e = new ResultAggregatorEntity(res.PartitionWithScheme, index, null, ETag = "*")
            let! bc = BlobCell.CreateIfNotExists(config, res.Container, fun () -> value)
            e.Uri <- bc.Uri.ToString()
            let! u = Table.replace config res.Table e
            return __.Complete
        }
    
    member __.Complete = Async.RunSync(completed())
    
    member __.ToArray() : Async<'T []> = 
        async { 
            if not __.Complete then 
                return! Async.Raise <| new InvalidOperationException("Result aggregator incomplete.")
            else
                let! xs = Table.queryPK<ResultAggregatorEntity> config res.Table res.PartitionWithScheme
                let bs = 
                    xs
                    |> Seq.sortBy (fun x -> x.Index)
                    |> Seq.map (fun x -> x.Uri)
                    |> Seq.map (fun x -> BlobCell<_>.OfUri(config, new Uri(x)))
                    |> Seq.toArray
            
                let re = Array.zeroCreate<'T> bs.Length
                let i = ref 0
                for b in bs do
                    let! v = b.GetValue()
                    re.[!i] <- v
                    incr i
                return re
        }
    
    member __.Uri = res

    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, context: StreamingContext): unit = 
            info.AddValue("uri", res, typeof<Uri>)
            info.AddValue("config", config, typeof<ConfigurationId>)

    new(info: SerializationInfo, context: StreamingContext) =
        let res = info.GetValue("uri", typeof<Uri>) :?> Uri
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new ResultAggregator<'T>(config, res)

    static member private GetUri<'T>(container, id) = uri "aggregator:%s/%s" container id
    static member Create<'T>(config, container : string, size : int) = 
        async { 
            let res = ResultAggregator<_>.GetUri(container, guid())
            for i = 0 to size - 1 do
                let e = new ResultAggregatorEntity(res.PartitionWithScheme, i, String.Empty)
                do! Table.insert config res.Table e
            return new ResultAggregator<'T>(config, res)
        }