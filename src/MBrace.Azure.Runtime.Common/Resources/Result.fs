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

type ResultCell<'T> internal (res : Uri) = 

    member __.SetResult(result : 'T) : Async<unit> =
        async {
            let! bc = BlobCell.Init(res.Container, fun () -> result)
            let uri = (bc :> IResource).Uri
            let e = new LightCellEntity(res.PartitionWithScheme, uri.ToString(), ETag = "*")
            let! u = Table.merge res.Table e
            return ()
        }

    member __.TryGetResult() : Async<'T option> = 
        async {
            let! e = Table.read<LightCellEntity> res.Table res.PartitionWithScheme ""
            if e.Uri = null then return None
            else
                let bc = BlobCell.OfUri<'T>(new Uri(e.Uri))
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

    new(info: SerializationInfo, context: StreamingContext) =
        let res = info.GetValue("uri", typeof<Uri>) :?> Uri
        new ResultCell<'T>(res)

    static member private GetUri(container, id) = uri "resultcell:%s/%s" container id
    static member FromUri<'T>(uri) = new ResultCell<'T>(uri)
    static member Init<'T>(container : string) : Async<ResultCell<'T>> = 
        async { 
            let res = ResultCell<_>.GetUri(container, guid())
            let e = new LightCellEntity(res.PartitionWithScheme, null)
            do! Table.insert<LightCellEntity> res.Table e
            return new ResultCell<'T>(res)
        }


type ResultAggregator<'T> internal (res : Uri, latch : Latch) = 
    
    member __.SetResult(index : int, value : 'T) : Async<bool> = 
        async { 
            let e = new ResultAggregatorEntity(res.PartitionWithScheme, index, null, ETag = "*")
            let! bc = BlobCell.Init(res.Container, fun () -> value)
            e.Uri <- (bc :> IResource).Uri.ToString()
            let! u = Table.replace res.Table e
            let! curr = latch.Decrement()
            return curr = 0
        }
    
    member __.Complete = latch.Value = 0
    
    member __.ToArray() : Async<'T []> = 
        async { 
            if not __.Complete then 
                return! Async.Raise <| new InvalidOperationException("Result aggregator incomplete.")
            else
                let! xs = Table.readBatch<ResultAggregatorEntity> res.Table res.PartitionWithScheme
                let bs = 
                    xs
                    |> Seq.filter (fun x -> x.RowKey <> "") // skip latch entity
                    |> Seq.sortBy (fun x -> x.Index)
                    |> Seq.map (fun x -> x.Uri)
                    |> Seq.map (fun x -> BlobCell<_>.OfUri(new Uri(x)))
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
            info.AddValue("latch", latch, typeof<Latch>)

    new(info: SerializationInfo, context: StreamingContext) =
        let res = info.GetValue("uri", typeof<Uri>) :?> Uri
        let latch = info.GetValue("latch", typeof<Latch>) :?> Latch
        new ResultAggregator<'T>(res, latch)

    static member private GetUri<'T>(container, id) = uri "aggregator:%s/%s" container id
    static member Init<'T>(container : string, size : int) = 
        async { 
            let res = ResultAggregator<_>.GetUri(container, guid())
            let! l = Latch.Init(res.Container, res.PartitionKey, size)
            for i = 0 to size - 1 do
                let e = new ResultAggregatorEntity(res.PartitionWithScheme, i, "")
                do! Table.insert res.Table e
            return new ResultAggregator<'T>(res, l)
        }