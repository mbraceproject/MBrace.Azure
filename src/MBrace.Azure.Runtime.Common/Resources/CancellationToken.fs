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
type DistributedCancellationTokenSource internal (config : ConfigurationId, pk, rk) = 
    let table = config.RuntimeTable

    let cancel () = async {
        // Cancel current
        let e = new CancellationTokenSourceEntity(pk, rk, Unchecked.defaultof<_>, IsCancellationRequested = true, ETag = "*")
        do! Table.merge config table e
            |> Async.Ignore
         
        // Gather all Uri's to update
        let visited = new HashSet<string>()
        let uris = new ResizeArray<string * string>()

        let rec walk pk rk = async {
            // Get all CTS links for current CTS.
            if visited.Add(rk) then
                let! e = Table.read<CancellationTokenSourceEntity> config table pk rk
                let ch = e.GetLinks()
                for (pk, rk) as e in ch do
                    if not <| visited.Contains(rk) then
                        uris.Add(e)
                        do! walk pk rk
        }
        do! walk pk rk
        
        do! uris
            |> Seq.groupBy (fun (pk, _) -> pk)
            |> Seq.map (fun (_, xs) -> 
                xs 
                |> Seq.map (fun (pk, rk) -> new CancellationTokenSourceEntity(pk, rk, Unchecked.defaultof<_>, IsCancellationRequested = true, ETag = "*"))
                |> Table.mergeBatch config table
                )
            |> Async.Parallel
            |> Async.Ignore
    }

    let mutable isCancellationRequested = false
    let check() = 
        async { 
            let! e = Table.read<CancellationTokenSourceEntity> config table pk rk
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
    
    override this.ToString() = sprintf "%s/%s" pk rk

    member this.PartitionKey = pk
    member this.RowKey = rk
    member this.Links = 
        Async.RunSync <| async {
            let! e = Table.read<CancellationTokenSourceEntity> config config.RuntimeTable pk rk
            return e.GetLinks()
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
        new DistributedCancellationTokenSource(config, pk, rk)

    static member FromPath(config : ConfigurationId, pid, rk) = new DistributedCancellationTokenSource(config, pid, rk)
    static member Create(config, pid, ?metadata, ?parent : DistributedCancellationTokenSource) = 
        async {
            // Create DCTS record 
            let childUri = guid()
            let ctse = new CancellationTokenSourceEntity(pid, childUri, List.empty) 
            metadata |> Option.iter(fun m -> ctse.Metadata <- m)      
            do! Table.insert<CancellationTokenSourceEntity> config config.RuntimeTable ctse
            // Create DCTS Link between child and parent, store in parent.
            match parent with
            | None -> ()
            | Some p -> 
                let! _ = Table.transact2<CancellationTokenSourceEntity> 
                            config config.RuntimeTable p.PartitionKey p.RowKey 
                            (fun p -> new CancellationTokenSourceEntity(p.PartitionKey, p.RowKey, (pid, childUri) :: p.GetLinks(), ETag = p.ETag))
                ()
            let dcts = new DistributedCancellationTokenSource(config, pid, childUri)
            // This step is essential. If parent is already canceled
            // then there is none to run Cancel and update future child tokens.
            match parent with
            | Some p when p.IsCancellationRequested -> dcts.Cancel()
            | _ -> ()
            return dcts
        }
