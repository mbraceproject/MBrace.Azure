namespace MBrace.Azure.Runtime

open System
open System.Runtime.Serialization
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities
open System.IO
open MBrace.Azure
open Microsoft.WindowsAzure.Storage.Blob
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table

// represents a value that has been persisted to blob store

[<DataContract>]
type BlobValue<'T> internal (config : ClusterId, prefix : string, filename : string) =
    
    [<DataMember(Name = "config")>]
    let config = config
    [<DataMember(Name = "prefix")>]
    let prefix = prefix
    [<DataMember(Name = "filename")>]
    let filename = filename

    member this.Size =
        let container = config.StorageAccount.BlobClient.GetContainerReference(config.RuntimeContainer)
        let blob = container.GetBlockBlobReference(sprintf "%s/%s" prefix filename)
        blob.FetchAttributes()
        blob.Properties.Length

    member __.GetValue() : Async<'T> = async { 
        let container = config.StorageAccount.BlobClient.GetContainerReference(config.RuntimeContainer)
        use! s = container.GetBlockBlobReference(sprintf "%s/%s" prefix filename).OpenReadAsync()
        return ProcessConfiguration.Serializer.Deserialize<'T>(s) 
    }

    /// <summary>
    ///  Try reading value, returning Some t if file exists, None if it doesn't exist.
    /// </summary>
    member __.TryGetValue() : Async<'T option> = async {
        let container = config.StorageAccount.BlobClient.GetContainerReference(config.RuntimeContainer)
        let! _ = container.CreateIfNotExistsAsyncSafe(maxRetries = 3)
        let b = container.GetBlockBlobReference(sprintf "%s/%s" prefix filename)
        let! exists = b.ExistsAsync()
        if exists then
            use! s = b.OpenReadAsync()
            let value = ProcessConfiguration.Serializer.Deserialize<'T>(s)
            return Some value
        else
            return None
    }
    
    member __.Path = sprintf "%s/%s" prefix filename

    static member FromPath(config : ClusterId, path : string) = 
        let p = path.Split('/')
        BlobValue<'T>.FromPath(config, p.[0], p.[1])

    static member FromPath(config : ClusterId, prefix, file) = 
        new BlobValue<'T>(config, prefix, file)

    static member Exists(config, prefix, filename) = async {
        let c = config.StorageAccount.BlobClient.GetContainerReference(config.RuntimeContainer)
        do! c.CreateIfNotExistsAsyncSafe(maxRetries = 3)
        let b = c.GetBlockBlobReference(sprintf "%s/%s" prefix filename)
        return! b.ExistsAsync()
    }

    static member Create(config, prefix, filename : string, f : unit -> 'T) = async { 
        let c = config.StorageAccount.BlobClient.GetContainerReference(config.RuntimeContainer)
        do! c.CreateIfNotExistsAsyncSafe(maxRetries = 3)
        let b = c.GetBlockBlobReference(sprintf "%s/%s" prefix filename)

        let options = BlobRequestOptions(ServerTimeout = Nullable<_>(TimeSpan.FromMinutes(40.)))
        use! stream = b.OpenWriteAsync(null, options, OperationContext(), Async.DefaultCancellationToken)
        ProcessConfiguration.Serializer.Serialize<'T>(stream, f())
        do! stream.FlushAsync()
        stream.Dispose()

        // For some reason large client uploads, fail to upload but do not throw exception...
        let! exists = b.ExistsAsync()
        if not exists then failwith(sprintf "Failed to upload %s/%s" prefix filename)

        return new BlobValue<'T>(config, prefix, filename)
    }


type Blob =

    static member Delete(account : AzureStorageAccount, container : string, path : string) = async {
        let c = account.BlobClient.GetContainerReference(container)
        let b = c.GetBlockBlobReference(path)
        let! _ = b.DeleteIfExistsAsync()
        return ()
    }        

    static member Exists(account : AzureStorageAccount, container : string, path : string) = async {
        let c = account.BlobClient.GetContainerReference(container)
        do! c.CreateIfNotExistsAsyncSafe(maxRetries = 3)
        let b = c.GetBlockBlobReference(path)
        return! b.ExistsAsync()
    }

    static member UploadFromFile(account : AzureStorageAccount, container : string, path : string, localPath : string) = async { 
        let c = account.BlobClient.GetContainerReference(container)
        do! c.CreateIfNotExistsAsyncSafe(maxRetries = 3)
        let b = c.GetBlockBlobReference(path)

        let options = BlobRequestOptions(ServerTimeout = Nullable<_>(TimeSpan.FromMinutes(40.)))
        do! b.UploadFromFileAsync(localPath, FileMode.Open, null, options, OperationContext(), Async.DefaultCancellationToken)

        // For some reason large client uploads, fail to upload but do not throw exception...
        let! exists = b.ExistsAsync()
        if not exists then raise <| System.IO.IOException(sprintf "Failed to upload %A" path)
    }