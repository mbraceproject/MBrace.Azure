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

[<DataContract>]
type Blob<'T> internal (config : ClusterState, prefix : string, filename : string) =
    
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
        let! _ = container.CreateIfNotExistsAsync()
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

    static member FromPath(config : ClusterState, path : string) = 
        let p = path.Split('/')
        Blob<'T>.FromPath(config, p.[0], p.[1])

    static member FromPath(config : ClusterState, prefix, file) = 
        new Blob<'T>(config, prefix, file)

    static member Exists(config, prefix, filename) = async {
        let c = config.StorageAccount.BlobClient.GetContainerReference(config.RuntimeContainer)
        let! _ = c.CreateIfNotExistsAsync()
        let b = c.GetBlockBlobReference(sprintf "%s/%s" prefix filename)
        return! b.ExistsAsync()
    }

    static member Create(config, prefix, filename : string, f : unit -> 'T) = async { 
        let c = config.StorageAccount.BlobClient.GetContainerReference(config.RuntimeContainer)
        let! _ = c.CreateIfNotExistsAsync()
        let b = c.GetBlockBlobReference(sprintf "%s/%s" prefix filename)

        let options = BlobRequestOptions(ServerTimeout = Nullable<_>(TimeSpan.FromMinutes(40.)))
        use! stream = b.OpenWriteAsync(null, options, OperationContext(), Async.DefaultCancellationToken)
        ProcessConfiguration.Serializer.Serialize<'T>(stream, f())
        do! stream.FlushAsync()
        stream.Dispose()

        // For some reason large client uploads, fail to upload but do not throw exception...
        let! exists = b.ExistsAsync()
        if not exists then failwith(sprintf "Failed to upload %s/%s" prefix filename)

        return new Blob<'T>(config, prefix, filename)
    }

[<DataContract>]
type Blob internal (config : ClusterState, prefix : string, filename : string) =
    
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

    member __.OpenRead() : Async<Stream> = async {
        let container = config.StorageAccount.BlobClient.GetContainerReference(config.RuntimeContainer)
        let ref = container.GetBlockBlobReference(sprintf "%s/%s" prefix filename)
        return! ref.OpenReadAsync()
    }
    
    member __.Path = sprintf "%s/%s" prefix filename

    static member Delete(config, file) = async {
        let c = config.StorageAccount.BlobClient.GetContainerReference(config.RuntimeContainer)
        let b = c.GetBlockBlobReference(file)
        let! _ = b.DeleteIfExistsAsync()
        return ()
    }        

    static member FromPath(config : ClusterState, path : string) = 
        let p = path.Split('/')
        Blob.FromPath(config, p.[0], p.[1])

    static member FromPath(config : ClusterState, prefix : string, file : string) =
        new Blob(config, prefix, file)

    static member Exists(config : ClusterState, prefix : string, filename : string) = async {
        let c = config.StorageAccount.BlobClient.GetContainerReference(config.RuntimeContainer)
        let! _ = c.CreateIfNotExistsAsync()
        let b = c.GetBlockBlobReference(sprintf "%s/%s" prefix filename)
        return! b.ExistsAsync()
    }

    static member UploadFromFile(config : ClusterState, prefix : string, filename : string, localPath : string) = async { 
        let c = config.StorageAccount.BlobClient.GetContainerReference(config.RuntimeContainer)
        let! _ = c.CreateIfNotExistsAsync()
        let b = c.GetBlockBlobReference(sprintf "%s/%s" prefix filename)

        let options = BlobRequestOptions(ServerTimeout = Nullable<_>(TimeSpan.FromMinutes(40.)))
        do! b.UploadFromFileAsync(localPath, FileMode.Open, null, options, OperationContext(), Async.DefaultCancellationToken)

        // For some reason large client uploads, fail to upload but do not throw exception...
        let! exists = b.ExistsAsync()
        if not exists then failwith(sprintf "Failed to upload %s/%s" prefix filename)

        return new Blob(config, prefix, filename)
    }