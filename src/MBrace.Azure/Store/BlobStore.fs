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
    
    let rootPath = "/"
    let rootPathAlt = @"\"
    let delims = [|'/'; '\\'|]

    let isPathRooted (path : string) =
        path.StartsWith rootPath || path.StartsWith rootPathAlt

    let normalizePath (path : string) =
        let normalized = path.Split(delims, StringSplitOptions.RemoveEmptyEntries) |> String.concat "/"
        "/" + normalized |> Uri.EscapeUriString

    let ensureRooted (path : string) =
        if isPathRooted path then path else raise <| FormatException(sprintf "Invalid path %A. Paths should start with '/' or '\\'." path)

    /// Azure blob container.
    type Container =
        | Root
        | Container of string
    
    /// Represents a 'directory' in blob storage.
    type StoreDirectory =
        {
            Container : Container
            SubDirectory : string option
        }

        static member Parse(path : string) =
            let path = ensureRooted path    
            let xs = path.Split(delims, 2, StringSplitOptions.RemoveEmptyEntries)
            match xs with
            | [||] -> { Container = Root; SubDirectory = None }
            | [|c|] -> 
                Validate.containerName c
                { Container = Container c ; SubDirectory = None }

            | [|c; x|] -> 
                Validate.containerName c
                { Container = Container c; SubDirectory = Some (x + "/") }

            | _ -> raise <| new FormatException(sprintf "Invalid store path %A." path)

        static member Validate(path : string) =
            ignore <| StoreDirectory.Parse path

    /// Represents a full path to a blob.
    type StorePath =
        {
            Container : Container
            BlobName : string 
        }

        static member Validate(path : string) =
            ignore <| StorePath.Parse path

        static member Parse(path : string) =
            let path = ensureRooted path    
            let xs = path.Split(delims, 2, StringSplitOptions.RemoveEmptyEntries)
            match xs with
            | [|x|] -> { Container = Root; BlobName = x }
            | [|c; x|] -> 
                Validate.containerName c
                { Container = Container c; BlobName = x }

            | _ -> raise <| new FormatException(sprintf "Invalid store path %A." path)

    /// <summary>
    ///     Creates a blob storage container reference given account and container name
    /// </summary>
    /// <param name="account">Storage account instance.</param>
    /// <param name="container">Container name</param>
    let getContainerReference (account : AzureStorageAccount) (container : Container) = async {
        let client = account.BlobClient
        match container with
        | Root -> 
            let root = client.GetRootContainerReference()
            do! root.CreateIfNotExistsAsyncSafe(maxRetries = 3)
            return root
        | Container c ->
            return client.GetContainerReference c
    }

    /// <summary>
    ///     Creates a blob reference given account and full path.
    /// </summary>
    /// <param name="account">Cloud storage account.</param>
    /// <param name="path">Path to blob.</param>
    let getBlobReference account (fullPath : string) = async {
        let path = StorePath.Parse fullPath
        let! container = getContainerReference account path.Container
        match path.Container with
        | Container _ -> do! container.CreateIfNotExistsAsyncSafe(maxRetries = 3)
        | Root -> ()

        return container.GetBlockBlobReference(path.BlobName)
    }

    /// Asynchronously lists blob items for given container and prefix
    let listBlobItemsAsync (container : CloudBlobContainer) (prefix : string option) = async {
        let fetchSegment (token : BlobContinuationToken) = async {
            let! segment = async {
                match prefix with
                | None -> return! container.ListBlobsSegmentedAsync(token)
                | Some prefix -> return! container.ListBlobsSegmentedAsync(prefix, token)
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
            let! (result : ContainerResultSegment) = client.ListContainersSegmentedAsync(token)
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
    let defaultContainer = normalizePath defaultContainer

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
            let path = normalizePath path
            let! blob = getBlobReference account path
            let! stream = blob.OpenWriteAsync()
            return stream :> Stream
        }
        
        member this.ReadETag(path: string, etag: ETag): Async<Stream option> = async {
            let path = normalizePath path
            let! blob = getBlobReference account path
            let! stream = async { return! blob.OpenReadAsync(AccessCondition.GenerateIfMatchCondition(etag), BlobRequestOptions(), null) } |> Async.Catch
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
            let path = normalizePath path
            let! blob = getBlobReference account path
            try
                do! blob.FetchAttributesAsync()
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

        member this.DefaultDirectory = defaultContainer
        member this.WithDefaultDirectory (newContainer : string) =
            let newContainer = normalizePath newContainer
            new BlobStore(account, newContainer) :> _

        member this.RootDirectory = rootPath
        member this.IsCaseSensitiveFileSystem = false

        member this.GetRandomDirectoryName() : string = normalizePath <| Guid.NewGuid().ToString()

        member this.IsPathRooted(path : string) = isPathRooted path
            
        member this.GetDirectoryName(path : string) = normalizePath <| Path.GetDirectoryName path

        member this.GetFileName(path : string) = Path.GetFileName(path)

        member this.Combine(paths : string []) : string = normalizePath <| Path.Combine paths

        member this.GetFileSize(path: string) : Async<int64> = async {
            let path = normalizePath path
            let! blob = getBlobReference account path
            let! result = Async.Catch <| Async.AwaitTaskCorrect(blob.FetchAttributesAsync())
            match result with
            | Choice1Of2 () when blob.Properties.Length = -1L -> return! Async.Raise <| FileNotFoundException(path)
            | Choice1Of2 () -> return blob.Properties.Length
            | Choice2Of2 ex when StoreException.NotFound ex -> return! Async.Raise <| FileNotFoundException(path)
            | Choice2Of2 ex -> return! Async.Raise ex
        }

        member this.GetLastModifiedTime (path: string, isDirectory : bool) : Async<DateTimeOffset> = async {
            let path = normalizePath path
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
            let path = normalizePath path
            let storePath = StorePath.Parse path
            let! container = getContainerReference account storePath.Container

            let! b1 = container.ExistsAsync()
            if b1 then
                let! blob = getBlobReference account path
                return! blob.ExistsAsync()
            else 
                return false
        }

        member this.EnumerateFiles(container : string) : Async<string []> = async {
            try
                let container = normalizePath container
                let path = StoreDirectory.Parse container
                let! containerRef = getContainerReference account path.Container
                let! listedBlobs = listBlobItemsAsync containerRef path.SubDirectory
                return 
                    listedBlobs
                    |> Seq.choose (function :? ICloudBlob as cb -> Some cb.Name | _ -> None)
                    |> Seq.map (fun b -> this.Combine(container, b))
                    |> Seq.map normalizePath
                    |> Seq.toArray

            with e when StoreException.NotFound e ->
                return raise <| new DirectoryNotFoundException(container, e)
        }
        
        member this.DeleteFile(path: string) : Async<unit> = async {
            try
                let path = normalizePath path
                let! blob = getBlobReference account path
                do! blob.DeleteAsync()
            with e when StoreException.NotFound e ->
                return raise <| new FileNotFoundException(path, e)
        }

        member this.DirectoryExists(container: string) : Async<bool> = async {
            let container = normalizePath container
            let path = StoreDirectory.Parse container
            match path.Container with
            | Container _ as c ->
                let! container = getContainerReference account c
                return! container.ExistsAsync()
            | Root ->
                return true
        }
        
        member this.CreateDirectory(container: string) : Async<unit> = async {
            let container = normalizePath container
            let path = StoreDirectory.Parse container
            match path.Container with
            | Container _ as c ->
                let! container = getContainerReference account c
                do! container.CreateIfNotExistsAsyncSafe(maxRetries = 3)
                return ()
            | Root -> 
                return ()
        }

        member this.DeleteDirectory(container: string, _recursiveDelete : bool) : Async<unit> = async {
            let container = normalizePath container
            let path = StoreDirectory.Parse container
            match path with
            | { Container = Root; SubDirectory = _ } -> return invalidArg "container" "Cannot delete the root container."
            | { Container = c; SubDirectory = None } ->
                let! container = getContainerReference account c
                let! _ = container.DeleteIfExistsAsync()
                return ()
            | { Container = c; SubDirectory = Some s } ->
                let! container = getContainerReference account c
                let sub = container.GetDirectoryReference(s)
                let! blobs = listSubdirBlobsAsync sub
                do! blobs |> Seq.map (fun b -> async {
                        let p = b.Uri.Segments |> Array.last
                        let! blob = getBlobReference account (normalizePath <| Path.Combine(container.Name,p))
                        let! _ = blob.DeleteIfExistsAsync()
                        ()
                    })
                    |> Async.Parallel
                    |> Async.Ignore
        }
        
        member this.EnumerateDirectories(directory : string) : Async<string []> = async {
            try
                let directory = normalizePath directory
                let path = StoreDirectory.Parse directory
                match path with
                | { Container = Root; SubDirectory = _ } ->
                    let! containers = listContainersAsync account.BlobClient
                    return containers |> Seq.map (fun c -> normalizePath c.Name) |> Seq.toArray

                | { Container = (Container cnt as c) ; SubDirectory = subdir } -> 
                    let! cref = getContainerReference account c
                    let! listedEntries = listBlobItemsAsync cref subdir
                    return
                        listedEntries
                        |> Seq.choose (function :? CloudBlobDirectory as d -> Some d | _ -> None)
                        |> Seq.map (fun d -> normalizePath <| Path.Combine(cnt, d.Prefix))
                        |> Seq.toArray

            with e when StoreException.NotFound e ->
                return raise <| new DirectoryNotFoundException(directory, e)
        }

        member this.WriteETag(path: string, writer : Stream -> Async<'R>) : Async<ETag * 'R> = async {
            let path = normalizePath path
            let! blob = getBlobReference account path
            // http://msdn.microsoft.com/en-us/library/azure/dd179431.aspx
            let! result = async {
                let options = BlobRequestOptions(ServerTimeout = Nullable<_>(TimeSpan.FromMinutes(40.)))
                use! stream = blob.OpenWriteAsync(null, options, OperationContext())
                return! writer(stream)
            }
            return blob.Properties.ETag, result
        } 
        
        member this.BeginRead(path: string) : Async<Stream> = async {
            try
                let path = normalizePath path
                let! blob = getBlobReference account path
                return! blob.OpenReadAsync()
            with e when StoreException.NotFound e ->
                return raise <| new FileNotFoundException(path, e)
        }

        member this.UploadFromStream(path: string, source: Stream) : Async<unit> = async {
            let path = normalizePath path
            let! blob = getBlobReference account path
            let options = BlobRequestOptions(ServerTimeout = Nullable<_>(TimeSpan.FromMinutes(40.)))
            do! blob.UploadFromStreamAsync(source, null, options, OperationContext()).ContinueWith ignore
        }
        
        member this.DownloadToStream(path: string, target: Stream) : Async<unit> = async {
            try
                let path = normalizePath path
                let! blob = getBlobReference account path
                do! blob.DownloadToStreamAsync(target).ContinueWith ignore
            with e when StoreException.NotFound e ->
                return raise <| new FileNotFoundException(path, e)
        }

        member this.UploadFromLocalFile(source : string, target : string) : Async<unit> = async {
            let target = normalizePath target
            let! blob = getBlobReference account target
            let options = BlobRequestOptions(ServerTimeout = Nullable<_>(TimeSpan.FromMinutes(40.)))
            do! blob.UploadFromFileAsync(source, FileMode.Open, null, options, OperationContext())
        }

        member this.DownloadToLocalFile(source : string, target : string) : Async<unit> = async {
            let source = normalizePath source
            let! blob = getBlobReference account source
            let! exists = blob.ExistsAsync()
            if not exists then raise <| new FileNotFoundException(source)
            let options = BlobRequestOptions(ServerTimeout = Nullable<_>(TimeSpan.FromMinutes(40.)))
            do! blob.DownloadToFileAsync(target, FileMode.Create, null, options, OperationContext())
        }