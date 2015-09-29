[<AutoOpen>]
module MBrace.Azure.Runtime.Utilities.StoreUtils

open System

open MBrace.Runtime.Utils.Retry

open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Microsoft.WindowsAzure.Storage.Blob

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

type CloudTable with
    /// CreatesIfNotExistAsync that protects from 409 conflict errors with supplied retry policy
    member table.CreateIfNotExistsAsyncSafe(?retryInterval : int, ?maxRetries : int) =
        retryAsync (mkStoreConflictRetryPolicy maxRetries retryInterval) 
                    (async { let! _ = table.CreateIfNotExistsAsync() in return () })

type CloudBlobContainer with
    /// CreatesIfNotExistAsync that protects from 409 conflict errors with supplied retry policy
    member container.CreateIfNotExistsAsyncSafe(?retryInterval : int, ?maxRetries : int) =
        retryAsync (mkStoreConflictRetryPolicy maxRetries retryInterval) 
                    (async { let! _ = container.CreateIfNotExistsAsync() in return () })