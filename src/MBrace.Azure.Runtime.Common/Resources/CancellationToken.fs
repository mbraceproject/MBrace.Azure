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
open MBrace.Azure

[<Sealed>]
type DistributedCancellationTokenSource internal (config : ConfigurationId, pk) = 
    let table = config.RuntimeTable

    let cancel () = async {
        // Cancel current
        let e = new CancellationTokenSourceEntity(pk, IsCancellationRequested = true, ETag = "*")
        do! Table.merge config table e
            |> Async.Ignore
         
        // Gather all Uri's to update
        let visited = new HashSet<string>()
        let uris = new ResizeArray<string>()

        let rec walk pk = async {
            // Get all CTS links for current CTS.
            if visited.Add(pk) then
                let! children = Table.queryPK<CancellationTokenLinkEntity> config table pk
                let ch = children 
                         |> Seq.filter (fun e -> e.RowKey <> String.Empty) // CTSEntity has empty RK
                         |> Seq.toArray
                for e in ch do
                    if not <| visited.Contains(e.RowKey) then
                        uris.Add(e.RowKey)
                        do! walk e.RowKey
        }
        do! walk pk
        
        do! uris
            |> Seq.map (fun pk -> async {
                let e = new CancellationTokenSourceEntity(pk, IsCancellationRequested = true, ETag = "*")
                do! Table.merge config table e
                    |> Async.Ignore })
            |> Async.Parallel
            |> Async.Ignore
    }

    let mutable isCancellationRequested = false
    let check() = 
        async { 
            let! e = Table.read<CancellationTokenSourceEntity> config table pk String.Empty
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
    
    override this.ToString() = pk

    member this.Path = pk

    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, _ : StreamingContext): unit = 
            info.AddValue("pk", pk, typeof<string>)
            info.AddValue("config", config, typeof<ConfigurationId>)

    new(info: SerializationInfo, _ : StreamingContext) =
        let pk = info.GetValue("pk", typeof<string>) :?> string
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new DistributedCancellationTokenSource(config, pk)

    static member FromPath(config : ConfigurationId, pk) = new DistributedCancellationTokenSource(config, pk)
    static member Create(config, ?metadata, ?parent : DistributedCancellationTokenSource) = 
        async {
            // Create DCTS record 
            let childUri = guid()
            let ctse = new CancellationTokenSourceEntity(childUri) 
            metadata |> Option.iter(fun m -> ctse.Metadata <- m)      
            do! Table.insert<CancellationTokenSourceEntity> config config.RuntimeTable ctse
            // Create DCTS Link between child and parent, store in parents table.
            match parent with
            | None -> ()
            | Some p -> 
                let link = new CancellationTokenLinkEntity(p.Path, childUri)
                do! Table.insert<CancellationTokenLinkEntity> config config.RuntimeTable link
            let dcts = new DistributedCancellationTokenSource(config, childUri)
            // This step is essential. If parent is already canceled
            // then there is none to run Cancel and update future child tokens.
            match parent with
            | Some p when p.IsCancellationRequested -> dcts.Cancel()
            | _ -> ()
            return dcts
        }
