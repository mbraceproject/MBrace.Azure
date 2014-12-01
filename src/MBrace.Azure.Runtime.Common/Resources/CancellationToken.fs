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

type DistributedCancellationTokenSource internal (config, res : Uri) = 
    let cancel () =
       async { 
            let e = new CancellationTokenSourceEntity(res.PartitionWithScheme, null, IsCancellationRequested = true, ETag = "*")
            let! u = Table.merge config res.Table e
            return ()
        }

    let check() = 
        async { 
            let! e = Table.read<CancellationTokenSourceEntity> config res.Table res.PartitionWithScheme ""
            if e.IsCancellationRequested then return true
            elif e.Link <> null then
                let link = new Uri(e.Link)
                let! p = Table.read<CancellationTokenSourceEntity> config link.Table link.PartitionWithScheme ""
                if p.IsCancellationRequested then
                    do! cancel ()
                    return true
                else return false
            else return false
        }
    
    let cts = lazy new CancellationTokenSource()
    
    interface IResource with
        member __.Uri = res
    
    member __.IsCancellationRequested = check() |> Async.RunSynchronously
    
    member __.Cancel() = cancel () |> Async.RunSynchronously
    
    member __.GetLocalCancellationToken() = 
        let rec loop () = async {
            let! isCancelled = check ()
            if isCancelled then
                cts.Value.Cancel()
            else
                do! Async.Sleep 500
                return! loop ()
        }

        Async.Start(loop())

        cts.Value.Token

    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, context: StreamingContext): unit = 
            info.AddValue("uri", res, typeof<Uri>)
            info.AddValue("config", config, typeof<ConfigurationId>)

    new(info: SerializationInfo, context: StreamingContext) =
        let res = info.GetValue("uri", typeof<Uri>) :?> Uri
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new DistributedCancellationTokenSource(config, res)

    static member private GetUri(container, id) = uri "dcts:%s/%s" container id
    static member FromUri(config : ConfigurationId, uri) = new DistributedCancellationTokenSource(config, uri)
    static member Create(config, container : string, ?parent : DistributedCancellationTokenSource) = 
        async { 
            let link = 
                match parent with
                | None -> null
                | Some p -> (p :> IResource).Uri.ToString()
            let res = DistributedCancellationTokenSource.GetUri(container, guid())
            let e = new CancellationTokenSourceEntity(res.PartitionWithScheme, link)
            do! Table.insert config res.Table e
            return new DistributedCancellationTokenSource(config, res)
        }
