namespace MBrace.Azure.Runtime.Utilities

open System
open System.IO
open System.Runtime.Serialization

open Microsoft.WindowsAzure.Storage.Blob
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table

open MBrace.Core.Internals
open MBrace.Runtime.Utils
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities
open MBrace.Azure

/// Represents a value that has been persisted to blob store
[<Sealed; DataContract>]
type BlobValue<'T> internal (account : AzureStorageAccount, container : string, path : string) =
    
    [<DataMember(Name = "account")>]
    let account = account
    [<DataMember(Name = "container")>]
    let container = container
    [<DataMember(Name = "path")>]
    let path = path

    /// Container to blob
    member __.Container = container
    /// Path to blob persisting the value
    member __.Path = path

    /// Asynchronously gets the blob size in bytes
    member __.GetSize() : Async<int64> = async {
        let container = account.BlobClient.GetContainerReference(container)
        let blob = container.GetBlockBlobReference(path)
        do! blob.FetchAttributesAsync() |> Async.AwaitTaskCorrect
        return blob.Properties.Length
    }

    /// Asynchronously gets the persisted value
    member __.GetValue() : Async<'T> = async { 
        let container = account.BlobClient.GetContainerReference(container)
        use! s = container.GetBlockBlobReference(path).OpenReadAsync() |> Async.AwaitTaskCorrect
        return ProcessConfiguration.BinarySerializer.Deserialize<'T>(s) 
    }

    /// Try reading the persisted value, returning Some t if file exists, None if it doesn't exist.
    member __.TryGetValue() : Async<'T option> = async {
        let container = account.BlobClient.GetContainerReference(container)
        let! _ = container.CreateIfNotExistsAsyncSafe(maxRetries = 3)
        let b = container.GetBlockBlobReference(path)
        let! exists = b.ExistsAsync() |> Async.AwaitTaskCorrect
        if exists then
            use! s = b.OpenReadAsync() |> Async.AwaitTaskCorrect
            let value = ProcessConfiguration.BinarySerializer.Deserialize<'T>(s)
            return Some value
        else
            return None
    }

    /// Asynchronously checks if blob exists in store
    member __.Exists() = async {
        let c = account.BlobClient.GetContainerReference(container)
        do! c.CreateIfNotExistsAsyncSafe(maxRetries = 3)
        let b = c.GetBlockBlobReference(path)
        return! b.ExistsAsync() |> Async.AwaitTaskCorrect
    }

    /// Asynchronously deletes the blob
    member __.Delete() = async {
        let c = account.BlobClient.GetContainerReference(container)
        let b = c.GetBlockBlobReference(path)
        let! _ = b.DeleteIfExistsAsync() |> Async.AwaitTaskCorrect
        return ()
    }        

    /// Asynchronously writes a value to the specified blob
    member __.WriteValue(value : 'T) = async { 
        let c = account.BlobClient.GetContainerReference(container)
        do! c.CreateIfNotExistsAsyncSafe(maxRetries = 3)
        let b = c.GetBlockBlobReference(path)

        let options = BlobRequestOptions(ServerTimeout = Nullable<_>(TimeSpan.FromMinutes(40.)))
        do! async {
            use! stream = b.OpenWriteAsync(null, options, OperationContext(), Async.DefaultCancellationToken) |> Async.AwaitTaskCorrect
            ProcessConfiguration.BinarySerializer.Serialize<'T>(stream, value)
        }

        // For some reason large client uploads, fail to upload but do not throw exception...
        let! exists = b.ExistsAsync() |> Async.AwaitTaskCorrect
        if not exists then 
            raise <| new System.IO.IOException(sprintf "Failed to upload %A" path)
    }

/// Represents a value that has been persisted to blob store
type BlobValue =

    /// <summary>
    ///     Defines a blob value with given parameters.
    /// </summary>
    /// <param name="account">Azure storage account.</param>
    /// <param name="container">Container to blob.</param>
    /// <param name="path">Blob path.</param>
    static member Define<'T>(account : AzureStorageAccount, container : string, path : string) = 
        new BlobValue<'T>(account, container, path)