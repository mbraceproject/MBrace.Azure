namespace MBrace.Azure.Runtime.Resources

open System
open System.Threading
open System.Runtime.Serialization
open MBrace.Azure.Runtime
open MBrace.Continuation
open MBrace.Azure.Runtime.Common
open Microsoft.WindowsAzure.Storage.Table
open MBrace
open System.Collections.Generic

[<Sealed>]
type DistributedCancellationTokenSource internal (config, uri : Uri) = 
    let cancel () = async {
        // Cancel current
        let e = new CancellationTokenSourceEntity(uri.SecondaryWithScheme, IsCancellationRequested = true, ETag = "*")
        do! Table.merge config uri.Primary e
            |> Async.Ignore
         
        // Gather all Uri's to update
        let visited = new HashSet<string>()
        let uris = new ResizeArray<string * string>()

        let rec walk table pk = async {
            // Get all CTS links for current CTS.
            if visited.Add(pk) then
                let! children = Table.queryPK<CancellationTokenLinkEntity> config table pk
                let ch = children 
                         |> Seq.filter (fun e -> e.RowKey <> String.Empty) // CTSEntity has empty RK
                         |> Seq.toArray
                for e in ch do
                    if not <| visited.Contains(e.RowKey) then
                        uris.Add(e.ChildTable, e.RowKey)
                        do! walk e.ChildTable e.RowKey
        }
        do! walk uri.Primary uri.SecondaryWithScheme
        
        do! uris
            |> Seq.map (fun (table, pk) -> async {
                let e = new CancellationTokenSourceEntity(pk, IsCancellationRequested = true, ETag = "*")
                do! Table.merge config table e
                    |> Async.Ignore })
            |> Async.Parallel
            |> Async.Ignore
    }

    let mutable isCancellationRequested = false
    let check() = 
        async { 
            let! e = Table.read<CancellationTokenSourceEntity> config uri.Primary uri.SecondaryWithScheme String.Empty
            isCancellationRequested <- e.IsCancellationRequested
            return isCancellationRequested
        }
    
    let localCts =
        lazy
            let cts = new CancellationTokenSource()
            let rec loop () = async {
                let! isCancelled = check ()
                if isCancelled then
                    cts.Cancel()
                else
                    do! Async.Sleep 200
                    return! loop ()
            }
            Async.Start(loop())
            cts


    interface ICloudCancellationToken with
        member this.IsCancellationRequested : bool = this.IsCancellationRequested
        member this.LocalToken : CancellationToken = localCts.Value.Token

    interface ICloudCancellationTokenSource with
        member this.Cancel() : unit = this.Cancel()
        member this.Token : ICloudCancellationToken = this :> ICloudCancellationToken

    member __.IsCancellationRequested = isCancellationRequested || Async.RunSync(check())
    
    member __.CancelAsync() = cancel ()
    member __.Cancel() = Async.RunSync(__.CancelAsync())
    member __.Uri = uri
    
    override this.ToString() = uri.ToString()

    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, _ : StreamingContext): unit = 
            info.AddValue("uri", uri, typeof<Uri>)
            info.AddValue("config", config, typeof<ConfigurationId>)

    new(info: SerializationInfo, _ : StreamingContext) =
        let uri = info.GetValue("uri", typeof<Uri>) :?> Uri
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new DistributedCancellationTokenSource(config, uri)

    static member private GetUri(container, id) = uri "dcts:%s/%s" container id
    static member FromUri(config : ConfigurationId, uri) = new DistributedCancellationTokenSource(config, uri)
    static member Create(config, container : string, ?metadata, ?parent : DistributedCancellationTokenSource) = 
        async {
            // Create DCTS record 
            let childUri = DistributedCancellationTokenSource.GetUri(container, guid())
            let ctse = new CancellationTokenSourceEntity(childUri.SecondaryWithScheme) 
            metadata |> Option.iter(fun m -> ctse.Metadata <- m)      
            do! Table.insert<CancellationTokenSourceEntity> config childUri.Primary ctse
            // Create DCTS Link between child and parent, store in parents table.
            match parent with
            | None -> ()
            | Some p -> 
                let link = new CancellationTokenLinkEntity(p.Uri.SecondaryWithScheme, childUri.SecondaryWithScheme, childUri.Primary)
                do! Table.insert<CancellationTokenLinkEntity> config p.Uri.Primary link
            let dcts = new DistributedCancellationTokenSource(config, childUri)
            // This step is essential. If parent is already canceled
            // then there is none to run Cancel and update future child tokens.
            match parent with
            | Some p when p.IsCancellationRequested -> dcts.Cancel()
            | _ -> ()
            return dcts
        }
