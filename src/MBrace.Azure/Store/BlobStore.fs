namespace MBrace.Azure.Store

open System
open System.IO
open System.Security.AccessControl
open System.Runtime.Serialization

open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Blob

open MBrace.Core.Internals
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities

[<AutoOpen>]
module private BlobUtils =
    let containerDelimiter = '@'

    let isWasbPath (path:string) =
        path
        |> Seq.filter ((=) containerDelimiter)
        |> Seq.length = 1

    let splitPath =
        let folderDelimiters = [| '@'; '/'; '\\'|]
        fun (path : string) ->
            if isWasbPath path then path.Split(folderDelimiters, 2, StringSplitOptions.RemoveEmptyEntries)
            else [| path |]

    /// Azure blob container.
    type Container = Container of string override this.ToString() = this |> function | Container x -> x

    type InvalidWasbPathException(path:string) =
        inherit Exception(sprintf "Invalid store path '%s'. A valid store path should confirm to WASB standard e.g. container@folder/folder/file.txt" path)
    
    /// Represents a 'directory' in blob storage.
    type StoreDirectory =
        {
            Container : Container
            SubDirectory : string option
        }

        static member Parse(path : string) =
            match splitPath path with
            | [|c|] -> 
                Validate.containerName c
                { Container = Container c ; SubDirectory = None }
            | [|c; x|] -> 
                Validate.containerName c
                { Container = Container c; SubDirectory = Some (x + "/") }
            | _ -> raise <| InvalidWasbPathException path

        static member Validate(path : string) =
            ignore <| StoreDirectory.Parse path

        static member Create(container : string, path : string option) =
            Validate.containerName container
            { Container = Container container; SubDirectory = path }

        override this.ToString() =
            match this.SubDirectory with
            | None -> string this.Container
            | Some subDirectory -> sprintf "%O@%s" this.Container subDirectory

    /// Represents a full path to a blob.
    type StorePath =
        {
            Container : Container
            BlobName : string 
        }

        static member Validate(path : string) =
            ignore <| StorePath.Parse path

        static member Parse(path : string) =
            match splitPath path with
            | [|c; x|] -> 
                Validate.containerName c
                { Container = Container c; BlobName = x }
            | _ -> raise <| InvalidWasbPathException path

        static member Create(container : string, path : string) =
            Validate.containerName container
            { Container = Container container; BlobName = path }

        override this.ToString() = sprintf "%O@%s" this.Container this.BlobName

    /// <summary>
    ///     Creates a blob storage container reference given account and container name
    /// </summary>
    /// <param name="account">Storage account instance.</param>
    /// <param name="container">Container name</param>
    let getContainerReference (account : AzureStorageAccount) (Container container) = async {
        let client = account.BlobClient
        let container = client.GetContainerReference container
        return container
    }

    let getBlobReferenceFull account fullPath ensureContainerExists = async {
        let path = StorePath.Parse fullPath
        let! container = getContainerReference account path.Container
        if ensureContainerExists then do! container.CreateIfNotExistsAsyncSafe(maxRetries = 3)
        return container.GetBlockBlobReference(path.BlobName) }

    /// <summary>
    ///     Creates a blob reference given account and full path.
    /// </summary>
    /// <param name="account">Cloud storage account.</param>
    /// <param name="path">Path to blob.</param>
    let getBlobReference account (fullPath : string) = getBlobReferenceFull account fullPath false

    /// Asynchronously lists blob items for given container and prefix
    let listBlobItemsAsync (container : CloudBlobContainer) (prefix : string option) = async {
        let fetchSegment (token : BlobContinuationToken) = async {
            let! segment = async {
                match prefix with
                | None -> return! container.ListBlobsSegmentedAsync(token) |> Async.AwaitTaskCorrect
                | Some prefix -> return! container.ListBlobsSegmentedAsync(prefix, token) |> Async.AwaitTaskCorrect
            }

            return segment.ContinuationToken, segment.Results
        }

        return! getSegmentedAsync fetchSegment
    }

    let listSubdirBlobsAsync (dir : CloudBlobDirectory) = async {
        let fetchSegment (token : BlobContinuationToken) = async {
            let! segment = dir.ListBlobsSegmentedAsync(true, BlobListingDetails.All, Nullable(), token, new BlobRequestOptions(), null) |> Async.AwaitTaskCorrect
            return segment.ContinuationToken, segment.Results
        }

        return! getSegmentedAsync fetchSegment
    }

    let listContainersAsync (client : CloudBlobClient) = async {
        let fetchSegment (token : BlobContinuationToken) = async {
            let! (result : ContainerResultSegment) = client.ListContainersSegmentedAsync(token) |> Async.AwaitTaskCorrect
            return result.ContinuationToken, result.Results
        }

        return! getSegmentedAsync fetchSegment
    }


///  MBrace File Store implementation that uses Azure Blob Storage as backend.
[<Sealed; DataContract>]
type BlobStore private (account : AzureStorageAccount, defaultContainer : string) =

    [<DataMember(Name = "StorageAccount")>]
    let account = account

    [<DataMember(Name = "DefaultContainer")>]
    let defaultContainer = defaultContainer

    do StoreDirectory.Validate defaultContainer

    /// <summary>
    ///     Creates an Azure blob store based CloudFileStore instance that connects to provided storage account.
    /// </summary>
    /// <param name="account">Azure storage account.</param>
    /// <param name="defaultContainer">Default container to be used be store instance.</param>
    static member Create(account : AzureStorageAccount, ?defaultContainer : string) = 
        ignore account.ConnectionString // force check that connection string is present in current host.
        new BlobStore(account, defaultArg defaultContainer "/")

    /// <summary>
    ///     Creates an Azure blob store based CloudFileStore instance that connects to provided connection string.
    /// </summary>
    /// <param name="connectionString">Azure storage account connection string.</param>
    /// <param name="defaultContainer">Default container to be used be store instance.</param>
    static member Create(connectionString : string, ?defaultContainer : string) = 
        BlobStore.Create(AzureStorageAccount.FromConnectionString connectionString, ?defaultContainer = defaultContainer)

    interface ICloudFileStore with
        member this.BeginWrite(path: string): Async<Stream> = async {
            let! blob = getBlobReferenceFull account path true
            let! stream = blob.OpenWriteAsync() |> Async.AwaitTaskCorrect
            return stream :> Stream
        }
        
        member this.ReadETag(path: string, etag: ETag): Async<Stream option> = async {
            let! blob = getBlobReference account path
            let! stream = blob.OpenReadAsync(AccessCondition.GenerateIfMatchCondition(etag), BlobRequestOptions(), null) |> Async.AwaitTaskCorrect |> Async.Catch
            match stream with
            | Choice1Of2 s -> 
                return Some s
            | Choice2Of2 e when StoreException.PreconditionFailed e -> 
                return None
            | Choice2Of2 e when StoreException.NotFound e ->
                return raise <| new FileNotFoundException(path, e)
            | Choice2Of2 e -> return raise e
        }

        member this.TryGetETag(path: string): Async<ETag option> = async {
            let! blob = getBlobReference account path
            try
                do! blob.FetchAttributesAsync() |> Async.AwaitTaskCorrect
                if String.IsNullOrEmpty blob.Properties.ETag then 
                    return None
                else
                    return Some blob.Properties.ETag
            with
            | ex when StoreException.NotFound ex -> return None
            | ex -> return raise ex
        }
        
        member this.Name = "MBrace.Azure.Store.BlobStore"
        member this.Id : string = account.CloudStorageAccount.BlobStorageUri.PrimaryUri.ToString()

        member this.DefaultDirectory = ""
        member this.WithDefaultDirectory (newContainer : string) =
            new BlobStore(account, newContainer) :> _

        member this.RootDirectory = ""
        member this.IsCaseSensitiveFileSystem = false

        member this.GetRandomDirectoryName() : string = Guid.NewGuid().ToString()

        member this.IsPathRooted(path : string) = isWasbPath path
            
        member this.GetDirectoryName(path : string) = Path.GetDirectoryName path

        member this.GetFileName(path : string) = Path.GetFileName(path)

        member this.Combine(paths : string []) : string = Path.Combine paths

        member this.GetFileSize(path: string) : Async<int64> = async {
            let! blob = getBlobReference account path
            let! result = Async.Catch <| Async.AwaitTaskCorrect(blob.FetchAttributesAsync())
            match result with
            | Choice1Of2 () when blob.Properties.Length = -1L -> return! Async.Raise <| FileNotFoundException(path)
            | Choice1Of2 () -> return blob.Properties.Length
            | Choice2Of2 ex when StoreException.NotFound ex -> return! Async.Raise <| FileNotFoundException(path)
            | Choice2Of2 ex -> return! Async.Raise ex
        }

        member this.GetLastModifiedTime (path: string, isDirectory : bool) : Async<DateTimeOffset> = async {
            if isDirectory then
                let storeDir = StoreDirectory.Parse path
                let! containerRef = getContainerReference account storeDir.Container
                let! result = Async.Catch <| Async.AwaitTaskCorrect(containerRef.FetchAttributesAsync())
                match result with
                | Choice1Of2 () ->
                    let lm = containerRef.Properties.LastModified
                    if lm.HasValue then return lm.Value
                    else return! Async.Raise <| DirectoryNotFoundException(path)

                | Choice2Of2 ex when StoreException.NotFound ex -> return! Async.Raise <| DirectoryNotFoundException(path)
                | Choice2Of2 ex -> return! Async.Raise ex
            else
                let! blob = getBlobReference account path
                let! result = Async.Catch <| Async.AwaitTaskCorrect(blob.FetchAttributesAsync())
                match result with
                | Choice1Of2 () -> 
                    let lm = blob.Properties.LastModified
                    if lm.HasValue then return lm.Value
                    else return! Async.Raise <| FileNotFoundException(path)

                | Choice2Of2 ex when StoreException.NotFound ex -> return! Async.Raise <| FileNotFoundException(path)
                | Choice2Of2 ex -> return! Async.Raise ex
        }

        member this.FileExists(path: string) : Async<bool> = async {
            let storePath = StorePath.Parse path
            let! container = getContainerReference account storePath.Container

            let! b1 = container.ExistsAsync() |> Async.AwaitTaskCorrect
            if b1 then
                let! blob = getBlobReference account path
                return! blob.ExistsAsync() |> Async.AwaitTaskCorrect
            else 
                return false
        }

        member this.EnumerateFiles(container : string) : Async<string []> = async {
            try
                let storeDirectory = StoreDirectory.Parse container
                let (Container container) = storeDirectory.Container
                let! containerRef = getContainerReference account storeDirectory.Container
                let! listedBlobs = listBlobItemsAsync containerRef storeDirectory.SubDirectory
                return 
                    listedBlobs
                    |> Seq.choose (function :? ICloudBlob as cb -> Some cb.Name | _ -> None)
                    |> Seq.map (fun b -> StorePath.Create(container, b) |> string)
                    |> Seq.toArray

            with e when StoreException.NotFound e ->
                return raise <| new DirectoryNotFoundException(container, e)
        }
        
        member this.DeleteFile(path: string) : Async<unit> = async {
            try
                let! blob = getBlobReference account path
                do! blob.DeleteAsync() |> Async.AwaitTaskCorrect
            with e when StoreException.NotFound e ->
                return ()
        }

        member this.DirectoryExists(container: string) : Async<bool> = async {
            let path = StoreDirectory.Parse container
            let! container = getContainerReference account path.Container
            return! container.ExistsAsync() |> Async.AwaitTaskCorrect
        }
        
        member this.CreateDirectory(container: string) : Async<unit> = async {
            let path = StoreDirectory.Parse container
            let! container = getContainerReference account path.Container
            do! container.CreateIfNotExistsAsyncSafe(maxRetries = 3)
            return ()
        }

        member this.DeleteDirectory(container: string, _recursiveDelete : bool) : Async<unit> = async {
            let path = StoreDirectory.Parse container
            match path with
            | { Container = c; SubDirectory = None } ->
                let! container = getContainerReference account c
                let! _ = container.DeleteIfExistsAsync() |> Async.AwaitTaskCorrect
                return ()
            | { Container = c; SubDirectory = Some s } ->
                let! container = getContainerReference account c
                let sub = container.GetDirectoryReference(s)
                let! blobs = listSubdirBlobsAsync sub
                do! blobs |> Seq.map (fun b -> async {
                        let p = b.Uri.Segments |> Array.last
                        let! blob = getBlobReference account (Path.Combine(container.Name,p))
                        let! _ = blob.DeleteIfExistsAsync() |> Async.AwaitTaskCorrect
                        ()
                    })
                    |> Async.Parallel
                    |> Async.Ignore
        }
        
        member this.EnumerateDirectories(directory : string) : Async<string []> = async {
            try
                let ({ Container = (Container cnt as c) ; SubDirectory = subdir }) = StoreDirectory.Parse directory
                let! cref = getContainerReference account c
                let! listedEntries = listBlobItemsAsync cref subdir
                return
                    listedEntries
                    |> Seq.choose (function :? CloudBlobDirectory as d -> Some d | _ -> None)
                    |> Seq.map (fun d -> StoreDirectory.Create(cnt, Some d.Prefix) |> string)
                    |> Seq.toArray

            with e when StoreException.NotFound e ->
                return raise <| new DirectoryNotFoundException(directory, e)
        }

        member this.WriteETag(path: string, writer : Stream -> Async<'R>) : Async<ETag * 'R> = async {
            let! blob = getBlobReference account path
            // http://msdn.microsoft.com/en-us/library/azure/dd179431.aspx
            let! result = async {
                let options = BlobRequestOptions(ServerTimeout = Nullable<_>(TimeSpan.FromMinutes(40.)))
                use! stream = blob.OpenWriteAsync(null, options, OperationContext()) |> Async.AwaitTaskCorrect
                return! writer(stream)
            }
            return blob.Properties.ETag, result
        } 
        
        member this.BeginRead(path: string) : Async<Stream> = async {
            try
                let! blob = getBlobReference account path
                return! blob.OpenReadAsync() |> Async.AwaitTaskCorrect
            with e when StoreException.NotFound e ->
                return raise <| new FileNotFoundException(path, e)
        }

        member this.UploadFromStream(path: string, source: Stream) : Async<unit> = async {
            let! blob = getBlobReferenceFull account path true
            let options = BlobRequestOptions(ServerTimeout = Nullable<_>(TimeSpan.FromMinutes(40.)))
            do! blob.UploadFromStreamAsync(source, null, options, OperationContext()).ContinueWith ignore |> Async.AwaitTaskCorrect
        }
        
        member this.DownloadToStream(path: string, target: Stream) : Async<unit> = async {
            try
                let! blob = getBlobReference account path
                do! blob.DownloadToStreamAsync(target) |> Async.AwaitTaskCorrect
            with e when StoreException.NotFound e ->
                return raise <| new FileNotFoundException(path, e)
        }

        member this.UploadFromLocalFile(source : string, target : string) : Async<unit> = async {
            let! blob = getBlobReferenceFull account target true
            let options = BlobRequestOptions(ServerTimeout = Nullable<_>(TimeSpan.FromMinutes(40.)))
            do! blob.UploadFromFileAsync(source, null, options, OperationContext()) |> Async.AwaitTaskCorrect
        }

        member this.DownloadToLocalFile(source : string, target : string) : Async<unit> = async {
            let! blob = getBlobReference account source
            let! exists = blob.ExistsAsync() |> Async.AwaitTaskCorrect
            if not exists then raise <| new FileNotFoundException(source)
            let options = BlobRequestOptions(ServerTimeout = Nullable<_>(TimeSpan.FromMinutes(40.)))
            do! blob.DownloadToFileAsync(target, FileMode.Create, null, options, OperationContext()) |> Async.AwaitTaskCorrect
        }