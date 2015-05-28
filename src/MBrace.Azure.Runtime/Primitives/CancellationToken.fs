namespace MBrace.Azure.Runtime.Primitives

open System
open System.Collections.Generic
open System.Threading
open System.Runtime.Serialization

open Microsoft.WindowsAzure.Storage.Table

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities

[<AutoOpen>]
module private CancellationTokenSourceUtils =

    /// cancels a distributed token entity and all its children
    let cancelTokenEntity (config : ConfigurationId) (partitionKey : string) (rowKey : string) = async {
        // Gather all Uris to update
        let visited = new HashSet<string * string>()
        let rec walk ((partitionKey, rowKey) as u) = async {
            if not <| visited.Contains u then
                // Get all CTS links for current CTS.
                let! e = Table.read<CancellationTokenSourceEntity> config config.RuntimeTable partitionKey rowKey
                if e.IsCancellationRequested then ()
                else
                    let _ = visited.Add u
                    for e' in e.GetLinks() do do! walk e'
        }

        do! walk (partitionKey, rowKey)
        
        do! visited
            |> Seq.groupBy (fun (partitionKey, _) -> partitionKey)
            |> Seq.map (fun (_, xs) -> 
                xs 
                |> Seq.map (fun (partitionKey, rowKey) -> new CancellationTokenSourceEntity(partitionKey, rowKey, Unchecked.defaultof<_>, IsCancellationRequested = true, ETag = "*"))
                |> Table.mergeBatch config config.RuntimeTable)
            |> Async.Parallel
            |> Async.Ignore
    }

    /// checks if token entity is cancellation requested
    let checkCancellation (config : ConfigurationId) (partitionKey : string) (rowKey : string) = async {
        let! e = Table.read<CancellationTokenSourceEntity> config config.RuntimeTable partitionKey rowKey
        return e.IsCancellationRequested
    }

    /// attempt to create a token entity; returns Some rowKey if successful or None if parent has already been canceled
    let tryCreateTokenEntity (config : ConfigurationId) (partitionKey : string) (metadata : string option) (parentUri : (string * string) option) = 
        async {
            // Create DCTS record
            let createEntry () = async {
                let childUri = guid()
                let ctse = new CancellationTokenSourceEntity(partitionKey, childUri, List.empty) 
                metadata |> Option.iter(fun m -> ctse.Metadata <- m)
                do! Table.insert<CancellationTokenSourceEntity> config config.RuntimeTable ctse
                return childUri
            }

            match parentUri with
            | None -> 
                let! uri = createEntry()
                return Some uri

            | Some (ppKey, prKey) -> 
                let! parent = Table.read<CancellationTokenSourceEntity> config config.RuntimeTable ppKey prKey
                if parent.IsCancellationRequested then return None
                else
                    let! uri = createEntry ()

                    let transact (p : CancellationTokenSourceEntity) =
                        new CancellationTokenSourceEntity(p.PartitionKey, p.RowKey, (partitionKey, uri) :: p.GetLinks(), ETag = p.ETag)

                    let! pt = Table.transact2<CancellationTokenSourceEntity> config config.RuntimeTable ppKey prKey transact

                    // This step is essential. If parent is already canceled
                    // then there is no need to run Cancel and update future child tokens.
                    if pt.IsCancellationRequested then do! cancelTokenEntity config partitionKey uri

                    return Some uri
        }

    let localTokens = new System.Collections.Concurrent.ConcurrentDictionary<string, CancellationToken> ()
    /// creates a cancellation token that is updated by polling the token entities
    let createLocalCancellationToken id checkCanceled =
        let ok, t = localTokens.TryGetValue id
        if ok then t
        elif Async.RunSync checkCanceled then
            new CancellationToken(canceled = true)
        else
            let createToken _ =
                let cts = new System.Threading.CancellationTokenSource()

                let rec checkCancellation () = async {
                    let! isCancelled = Async.Catch checkCanceled
                    match isCancelled with
                    | Choice1Of2 true -> 
                        cts.Cancel()
                        localTokens.TryRemove id |> ignore
                    | Choice1Of2 false ->
                        do! Async.Sleep 200
                        return! checkCancellation ()
                    | Choice2Of2 _ ->
                        do! Async.Sleep 1000
                        return! checkCancellation ()
                }

                do Async.Start(checkCancellation())
                cts.Token

            localTokens.AddOrUpdate(id, createToken, fun _ t -> t)

/// Distributed ICloudCancellationTokenSource implementation
[<DataContract; Sealed>] 
type DistributedCancellationTokenSource private (config : ConfigurationId, partitionKey : string, state : CancellationTokenState) = 

    // A DistributedCancellationTokenSource has three possible internal states: Distributed, Local and Canceled
    // A local state is just an in-memory cancellation token without storage backing
    // Cancellation tokens can be elevated to distributed state by persisting to the table storage
    // This can happen either 1) upon creation 2) manually or 3) automatically on serialization of the object
    // This is to minimize communication for local-only cancellation tokens that can still acquire potentially global range.

    [<DataMember(Name = "config")>]
    let config = config
    [<DataMember(Name = "partitionKey")>]
    let partitionKey = partitionKey
    [<DataMember(Name = "state")>]
    let mutable state = state

    // nonserializable cancellation token source that is initialized only in case of localized semantics
    [<IgnoreDataMember>]
    let localCancellationTokenSource =
        match state with
        | Localized (_, None) -> new CancellationTokenSource()
        | Localized (_, Some parent) -> CancellationTokenSource.CreateLinkedTokenSource [| parent.LocalToken |]
        | _ -> null

    // local cancellation token instace for source    
    [<IgnoreDataMember>]
    let mutable localToken : CancellationToken option = None
    // resolves local token for the instance
    let getLocalToken () =
        match localToken with
        | Some ct -> ct
        | None ->
            lock state (fun () ->
                match localToken with
                | Some ct -> ct
                | None ->
                    let ct =
                        match state with
                        | Canceled -> new CancellationToken(canceled = true)
                        | Localized _ -> localCancellationTokenSource.Token
                        | Distributed rowKey -> createLocalCancellationToken (partitionKey + ":" + rowKey) (checkCancellation config partitionKey rowKey)

                    localToken <- Some ct
                    ct)

    /// Attempt to elevate cancellation token to global range
    member private c.TryElevateToDistributed() =
        match state with
        | Canceled -> None
        | Distributed rk -> Some rk
        | Localized _ ->
            lock state (fun () ->
                match state with
                | Canceled -> None
                | Distributed rk -> Some rk
                | Localized (metadata, None) ->
                    match tryCreateTokenEntity config partitionKey metadata None |> Async.RunSync with
                    | None -> state <- Canceled ; None
                    | Some rowKey -> state <- Distributed rowKey ; localToken <- None ; Some rowKey

                | Localized (metadata, Some parent) ->
                    // elevate parent to distributed source
                    match parent.TryElevateToDistributed() with
                    | None -> state <- Canceled ; None // parent canceled ; declare canceled
                    | Some parentRowKey ->
                        // create token entity using for current cts
                        match tryCreateTokenEntity config partitionKey metadata (Some (parent.PartitionKey, parentRowKey)) |> Async.RunSync with
                        | None -> state <- Canceled ; None
                        | Some rowKey -> state <- Distributed rowKey ; localToken <- None ; Some rowKey)

    /// Triggers elevation in event of serialization
    [<OnSerializing>]
    member private c.OnDeserializing (_ : StreamingContext) = c.TryElevateToDistributed() |> ignore

    /// Elevates cancellation token to global scope. Returns true if succesful, false if already canceled.
    member __.ElevateCancellationToken () = __.TryElevateToDistributed() |> Option.isSome

    member this.PartitionKey = partitionKey
    member this.RowKey = 
        match state with 
        | Distributed rK -> Some rK 
        | _ -> None

    member __.LocalToken = getLocalToken()
    member __.IsCancellationRequested = getLocalToken().IsCancellationRequested
    member __.Cancel() =
        if getLocalToken().IsCancellationRequested then ()
        else
            lock state (fun () ->
                match state with
                | Canceled _ -> ()
                | Localized _ -> localCancellationTokenSource.Cancel()
                | Distributed rK -> cancelTokenEntity config partitionKey rK |> Async.RunSync)

    interface ICloudCancellationToken with
        member this.IsCancellationRequested : bool = this.IsCancellationRequested
        member this.LocalToken : CancellationToken = getLocalToken ()

    interface ICloudCancellationTokenSource with
        member this.Cancel() : unit = this.Cancel()
        member this.Token : ICloudCancellationToken = this :> ICloudCancellationToken

    
    override this.ToString() = 
        match state with
        | Canceled 
        | Localized _ -> sprintf "Local cancellation token %O" <| getLocalToken()
        | Distributed rowKey -> sprintf "Table store cancellation token:%s/%s" partitionKey rowKey

    member this.TryGetLinkedCancellationTokens () = 
        Async.RunSync <| async {
            match state with
            | Distributed rowKey ->
                let! e = Table.read<CancellationTokenSourceEntity> config config.RuntimeTable partitionKey rowKey
                return Some <| e.GetLinks()
            | _ -> return None
        }

    static member FromPath(config : ConfigurationId, pid : string, rowKey : string) =
        new DistributedCancellationTokenSource(config, pid, Distributed rowKey)

    static member Create(config : ConfigurationId, pid : string, ?metadata : string, ?parent : DistributedCancellationTokenSource, ?elevate : bool) = async {
        let elevate = defaultArg elevate false
        match parent with
        | None when elevate ->
            let! rKey = tryCreateTokenEntity config pid metadata None
            return new DistributedCancellationTokenSource(config, pid, Distributed <| Option.get rKey)
        | _ ->
            let dcts = new DistributedCancellationTokenSource(config, pid, Localized(metadata, parent))
            if elevate then dcts.TryElevateToDistributed() |> ignore
            return dcts
    }

and private CancellationTokenState =
    | Canceled // Note that dcts's will carry the cancelled state only if cancellation has occured 
               // *before* elevation to table storage. This is to ensure state consistency across copies 
               // of the the token.
    | Distributed of rowKey:string
    | Localized of metadata:string option * parent:DistributedCancellationTokenSource option