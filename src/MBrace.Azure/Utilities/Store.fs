[<AutoOpen>]
module MBrace.Azure.Runtime.Utilities.StoreUtils

open System
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