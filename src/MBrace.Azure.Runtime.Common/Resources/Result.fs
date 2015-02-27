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

type ResultCell<'T> internal (config : ConfigurationId, pk) as self = 
    let table = config.RuntimeTable
    let localCell = lazy CacheAtom.Create((fun () -> self.TryGetResult() |> Async.RunSync), intervalMilliseconds = 200)

    member this.Path = pk

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
            let! bc = Blob.Create(config, fun () -> result)
            let uri = bc.Filename
            let e = new LightCellEntity(pk, uri.ToString(), ETag = "*")
            let! _ = Table.merge config table e
            return ()
        }

    member __.TryGetResult() : Async<Result<'T> option> = 
        async {
            let! e = Table.read<LightCellEntity> config table pk String.Empty
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
            info.AddValue("config", config, typeof<ConfigurationId>)

    new(info: SerializationInfo, _ : StreamingContext) =
        let pk = info.GetValue("pk", typeof<string>) :?> string
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new ResultCell<'T>(config, pk)

    static member FromPath(config : ConfigurationId, uri) = new ResultCell<'T>(config, uri)
    static member Create(config, id) : Async<ResultCell<'T>> = 
        async { 
            let e = new LightCellEntity(id, null)
            do! Table.insert<LightCellEntity> config config.RuntimeTable e
            return new ResultCell<'T>(config, id)
        }


type ResultAggregator<'T> internal (config : ConfigurationId, pk) = 
    let table = config.RuntimeTable

    let completed () =
        async {
            let! xs = Table.queryPK<ResultAggregatorEntity> config table pk
            return xs |> Seq.forall (fun e -> e.Uri <> String.Empty)
        }

    member __.SetResult(index : int, value : 'T) : Async<bool> = 
        async { 
            let e = new ResultAggregatorEntity(pk, index, null, ETag = "*")
            let! bc = Blob.Create(config, fun () -> value)
            e.Uri <- bc.Filename
            let! _ = Table.replace config table e
            return __.Complete
        }
    
    member __.Complete = Async.RunSync(completed())
    
    member __.ToArray() : Async<'T []> = 
        async { 
            if not __.Complete then 
                return! Async.Raise <| new InvalidOperationException("Result aggregator incomplete.")
            else
                let! xs = Table.queryPK<ResultAggregatorEntity> config table pk
                let bs = 
                    xs
                    |> Seq.sortBy (fun x -> x.Index)
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
            info.AddValue("config", config, typeof<ConfigurationId>)

    new(info: SerializationInfo, _: StreamingContext) =
        let pk = info.GetValue("pk", typeof<string>) :?> string
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new ResultAggregator<'T>(config, pk)

    static member Create<'T>(config, size : int) = 
        async { 
            let pk = guid()
            let entities = seq {
                for i = 0 to size - 1 do
                    yield new ResultAggregatorEntity(pk, i, String.Empty)
            }
                do! Table.insertBatch config config.RuntimeTable entities
            return new ResultAggregator<'T>(config, pk)
        }