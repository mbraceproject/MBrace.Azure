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

/// Azure Table Store implementation of ICloudCancellationTokenSource
[<DataContract; Sealed>]
type DistributedCancellationTokenSource internal (config : ConfigurationId, partitionKey : string , rowKey : string) = 

    [<DataMember(Name = "config")>]
    let config = config
    [<DataMember(Name = "partitionKey")>]
    let partitionKey = partitionKey
    [<DataMember(Name = "rowKey")>]
    let rowKey = rowKey
    [<DataMember(Name = "isCancelled")>]
    let mutable isCancellationRequested = false
    
    [<IgnoreDataMember>]
    let mutable localToken : CancellationToken option = None

    let cancel () = async {
        // Cancel current
        let e = new CancellationTokenSourceEntity(partitionKey, rowKey, Unchecked.defaultof<_>, IsCancellationRequested = true, ETag = "*")
        do! Table.merge config config.RuntimeTable e
            |> Async.Ignore
         
        // Gather all Uri's to update
        let visited = new HashSet<string>()
        let uris = new ResizeArray<string * string>()

        let rec walk partitionKey rowKey = async {
            // Get all CTS links for current CTS.
            if visited.Add(rowKey) then
                let! e = Table.read<CancellationTokenSourceEntity> config config.RuntimeTable partitionKey rowKey
                let ch = e.GetLinks()
                for (partitionKey, rowKey) as e in ch do
                    if not <| visited.Contains(rowKey) then
                        uris.Add(e)
                        do! walk partitionKey rowKey
        }
        do! walk partitionKey rowKey
        
        do! uris
            |> Seq.groupBy (fun (partitionKey, _) -> partitionKey)
            |> Seq.map (fun (_, xs) -> 
                xs 
                |> Seq.map (fun (partitionKey, rowKey) -> new CancellationTokenSourceEntity(partitionKey, rowKey, Unchecked.defaultof<_>, IsCancellationRequested = true, ETag = "*"))
                |> Table.mergeBatch config config.RuntimeTable
                )
            |> Async.Parallel
            |> Async.Ignore
    }

    let check() = 
        async { 
            let! e = Table.read<CancellationTokenSourceEntity> config config.RuntimeTable partitionKey rowKey
            isCancellationRequested <- e.IsCancellationRequested
            return isCancellationRequested
        }

    let getLocalToken () =
        match localToken with
        | Some t -> t
        | None when isCancellationRequested -> 
            let t = new CancellationToken(true)
            localToken <- Some t
            t
        | None ->
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
            localToken <- Some cts.Token
            cts.Token

    interface ICloudCancellationToken with
        member this.IsCancellationRequested : bool = this.IsCancellationRequested
        member this.LocalToken : CancellationToken = getLocalToken ()

    interface ICloudCancellationTokenSource with
        member this.Cancel() : unit = this.Cancel()
        member this.Token : ICloudCancellationToken = this :> ICloudCancellationToken

    member __.IsCancellationRequested = isCancellationRequested || Async.RunSync(check())
    
    member __.CancelAsync() = cancel ()
    member __.Cancel() = Async.RunSync(__.CancelAsync())
    
    override this.ToString() = sprintf "%s/%s" partitionKey rowKey

    member this.PartitionKey = partitionKey
    member this.RowKey = rowKey
    member this.Links = 
        Async.RunSync <| async {
            let! e = Table.read<CancellationTokenSourceEntity> config config.RuntimeTable partitionKey rowKey
            return e.GetLinks()
        }

    static member FromPath(config : ConfigurationId, pid, rowKey) = new DistributedCancellationTokenSource(config, pid, rowKey)
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
