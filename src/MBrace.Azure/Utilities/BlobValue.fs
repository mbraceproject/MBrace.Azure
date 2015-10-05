namespace MBrace.Azure.Runtime.Utilities

open System
open System.Runtime.Serialization
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities
open System.IO
open MBrace.Azure
open Microsoft.WindowsAzure.Storage.Blob
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table

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
        do! blob.FetchAttributesAsync()
        return blob.Properties.Length
    }

    /// Asynchronously gets the persisted value
    member __.GetValue() : Async<'T> = async { 
        let container = account.BlobClient.GetContainerReference(container)
        use! s = container.GetBlockBlobReference(path).OpenReadAsync()
        return ProcessConfiguration.Serializer.Deserialize<'T>(s) 
    }

    /// Try reading the persisted value, returning Some t if file exists, None if it doesn't exist.
    member __.TryGetValue() : Async<'T option> = async {
        let container = account.BlobClient.GetContainerReference(container)
        let! _ = container.CreateIfNotExistsAsyncSafe(maxRetries = 3)
        let b = container.GetBlockBlobReference(path)
        let! exists = b.ExistsAsync()
        if exists then
            use! s = b.OpenReadAsync()
            let value = ProcessConfiguration.Serializer.Deserialize<'T>(s)
            return Some value
        else
            return None
    }

    /// Asynchronously checks if blob exists in store
    member __.Exists() = async {
        let c = account.BlobClient.GetContainerReference(container)
        do! c.CreateIfNotExistsAsyncSafe(maxRetries = 3)
        let b = c.GetBlockBlobReference(path)
        return! b.ExistsAsync()
    }

    /// Asynchronously deletes the blob
    member __.Delete() = async {
        let c = account.BlobClient.GetContainerReference(container)
        let b = c.GetBlockBlobReference(path)
        let! _ = b.DeleteIfExistsAsync()
        return ()
    }        

    /// Asynchronously writes a value to the specified blob
    member __.WriteValue(value : 'T) = async { 
        let c = account.BlobClient.GetContainerReference(container)
        do! c.CreateIfNotExistsAsyncSafe(maxRetries = 3)
        let b = c.GetBlockBlobReference(path)

        let options = BlobRequestOptions(ServerTimeout = Nullable<_>(TimeSpan.FromMinutes(40.)))
        use! stream = b.OpenWriteAsync(null, options, OperationContext(), Async.DefaultCancellationToken)
        ProcessConfiguration.Serializer.Serialize<'T>(stream, value)
        do! stream.FlushAsync()
        stream.Dispose()

        // For some reason large client uploads, fail to upload but do not throw exception...
        let! exists = b.ExistsAsync()
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