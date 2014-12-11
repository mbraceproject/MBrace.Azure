namespace Nessos.MBrace.Azure.Runtime.Resources

open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Common
open Nessos.MBrace.Azure.Runtime.Resources
open Nessos.MBrace.Runtime
open Nessos.Vagrant
open System
open System.Runtime.Serialization
open System.Threading
open Nessos.MBrace.Azure.Runtime.Common.Storage
open Nessos.MBrace.Continuation

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

type ResultCell<'T> internal (config, res : Uri) = 

    member __.SetResult(result : 'T) : Async<unit> =
        async {
            let! bc = BlobCell.CreateIfNotExists(config, res.Container, fun () -> result)
            let uri = (bc :> IResource).Uri
            let e = new LightCellEntity(res.PartitionWithScheme, uri.ToString(), ETag = "*")
            let! u = Table.merge config res.Table e
            return ()
        }

    member __.TryGetResult() : Async<'T option> = 
        async {
            let! e = Table.read<LightCellEntity> config res.Table res.PartitionWithScheme ""
            if e.Uri = null then return None
            else
                let bc = BlobCell.OfUri<'T>(config, new Uri(e.Uri))
                let! v = bc.GetValue()
                return Some v
        }
    
    member __.AwaitResult() : Async<'T> = 
        async { 
            let! r = __.TryGetResult()
            match r with
            | None -> return! __.AwaitResult()
            | Some r -> return r
        }
    
    interface IResource with
        member __.Uri = res

    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, context: StreamingContext): unit = 
            info.AddValue("uri", res, typeof<Uri>)
            info.AddValue("config", config, typeof<ConfigurationId>)

    new(info: SerializationInfo, context: StreamingContext) =
        let res = info.GetValue("uri", typeof<Uri>) :?> Uri
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new ResultCell<'T>(config, res)

    static member private GetUri(container, id) = uri "resultcell:%s/%s" container id
    static member FromUri<'T>(config : ConfigurationId, uri) = new ResultCell<'T>(config, uri)
    static member Create<'T>(config, container : string) : Async<ResultCell<'T>> = 
        async { 
            let res = ResultCell<_>.GetUri(container, guid())
            let e = new LightCellEntity(res.PartitionWithScheme, null)
            do! Table.insert<LightCellEntity> config res.Table e
            return new ResultCell<'T>(config, res)
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
            e.Uri <- (bc :> IResource).Uri.ToString()
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
    
    interface IResource with 
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