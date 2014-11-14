namespace Nessos.MBrace.Azure.Runtime.Resources

open System
open System.Threading
open System.Runtime.Serialization
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Common
open Microsoft.WindowsAzure.Storage.Table

// Note : Each dcts checks for cancelation itself and parent dcts (but no other predecessors)
// This works as long as all parent dcts run the GetLocalCancellationToken loop.
// Fault tolerance?

type DistributedCancellationTokenSource internal (res : Uri) = 
    let cancel () =
       async { 
            let e = new CancellationTokenSourceEntity(res.PartitionWithScheme, null, IsCancellationRequested = true, ETag = "*")
            let! u = Table.merge res.Table e
            return ()
        }

    let check() = 
        async { 
            let! e = Table.read<CancellationTokenSourceEntity> res.Table res.PartitionWithScheme ""
            if e.IsCancellationRequested then return true
            elif e.Link <> null then
                let link = new Uri(e.Link)
                let! p = Table.read<CancellationTokenSourceEntity> link.Table link.PartitionWithScheme ""
                if p.IsCancellationRequested then
                    do! cancel ()
                    return true
                else return false
            else return false
        }
    
    interface IResource with
        member __.Uri = res
        member __.Dispose () = 
            Table.delete res.Table (new TableEntity(res.PartitionWithScheme, ""))
            |> Async.RunSynchronously
    
    member __.IsCancellationRequested = check() |> Async.RunSynchronously
    
    member __.Cancel() = cancel () |> Async.RunSynchronously
    
    member __.GetLocalCancellationToken() = 
        let cts = new CancellationTokenSource()

        let rec loop () = async {
            let! isCancelled = check ()
            if isCancelled then
                cts.Cancel()
            else
                do! Async.Sleep 500
                return! loop ()
        }

        Async.Start(loop())

        cts.Token

    static member Init(res : Uri, ?parent : DistributedCancellationTokenSource) = 
        async { 
            let link = 
                match parent with
                | None -> null
                | Some p -> (p :> IResource).Uri.ToString()
            
            let e = new CancellationTokenSourceEntity(res.PartitionWithScheme, link)
            do! Table.insert res.Table e
            return new DistributedCancellationTokenSource(res)
        }
    
    static member Get(res : Uri) = new DistributedCancellationTokenSource(res)
    static member GetUri(container, id) = uri "dcts:%s/%s" container id
    static member GetUri(container) = DistributedCancellationTokenSource.GetUri(container, guid())

    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, context: StreamingContext): unit = 
            info.AddValue("uri", res, typeof<Uri>)

    new(info: SerializationInfo, context: StreamingContext) =
        let res = info.GetValue("uri", typeof<Uri>) :?> Uri
        new DistributedCancellationTokenSource(res)