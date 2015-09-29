namespace MBrace.Azure.Runtime.Utilities

open System
open Microsoft.WindowsAzure.Storage

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