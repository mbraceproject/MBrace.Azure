[<AutoOpen>]
module MBrace.Azure.Runtime.Utilities.StoreUtils

open System

open MBrace.Core.Internals
open MBrace.Runtime.Utils.Retry

open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Microsoft.WindowsAzure.Storage.Blob
open Microsoft.ServiceBus.Messaging

[<RequireQualifiedAccess>]
module StoreException =

    let private checkExn code (e : exn) =
        match e with
        | :? StorageException as e -> e.RequestInformation.HttpStatusCode = code
        | :? AggregateException as e ->
            match e.InnerException with
            | :? StorageException as e -> e.RequestInformation.HttpStatusCode = code
            | _ -> false
        | _ -> false
    
    let PreconditionFailed (e : exn) = checkExn 412 e
    let Conflict (e : exn) = checkExn 409 e
    let NotFound (e : exn) = checkExn 404 e


let private mkStoreConflictRetryPolicy maxRetries interval =
    let interval = defaultArg interval 3000 |> float |> TimeSpan.FromMilliseconds
    Policy (fun retries exn ->
        if maxRetries |> Option.exists (fun mr -> retries > mr) then None
        elif StoreException.Conflict exn then Some interval
        else None)

/// generic utility function for asynchronous getting of blob segmented APIs
let getSegmentedAsync (getter : BlobContinuationToken -> Async<BlobContinuationToken * #seq<'T>>) = async {
    let acc = new ResizeArray<'T> ()
    let rec aux (token : BlobContinuationToken) = async {
        let! token', partial = getter token
        acc.AddRange partial
        if token' = null then return ()
        else return! aux token'
    }

    do! aux null
    return acc :> seq<'T>
}

type CloudTable with
    /// CreatesIfNotExistAsync that protects from 409 conflict errors with supplied retry policy
    member table.CreateIfNotExistsAsyncSafe(?retryInterval : int, ?maxRetries : int) =
        retryAsync (mkStoreConflictRetryPolicy maxRetries retryInterval) 
                    (async { let! _ = table.CreateIfNotExistsAsync() |> Async.AwaitTaskCorrect in return () })

type CloudBlobContainer with
    /// CreatesIfNotExistAsync that protects from 409 conflict errors with supplied retry policy
    member container.CreateIfNotExistsAsyncSafe(?retryInterval : int, ?maxRetries : int) =
        retryAsync (mkStoreConflictRetryPolicy maxRetries retryInterval) 
                    (async { let! _ = container.CreateIfNotExistsAsync() |> Async.AwaitTaskCorrect in return () })

type BrokeredMessage with
    member message.TryGetProperty<'T>(id : string) =
        let mutable result = Unchecked.defaultof<obj>
        if message.Properties.TryGetValue(id, &result) then
            Some(result :?> 'T)
        else
            None

    member message.GetProperty<'T>(id : string) =
        message.Properties.[id] :?> 'T