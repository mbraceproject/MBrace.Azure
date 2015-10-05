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
        let path = if isPathRooted path then path else Path.Combine(rootPath, path)
        path.Replace('\\', '/')

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
    with
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
                { Container = Container c; SubDirectory = Some x }

            | _ -> raise <| new FormatException(sprintf "Invalid store path %A." path)

    /// Represents a full path to a blob.
    type StorePath =
        {
            Container : Container
            BlobName : string 
        }
    with
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


///  MBrace File Store implementation that uses Azure Blob Storage as backend.
[<Sealed; DataContract>]
type BlobStore private (account : AzureStorageAccount, defaultContainer : string) =

    [<DataMember(Name = "StorageAccount")>]
    let account = account

    [<DataMember(Name = "DefaultContainer")>]
    let defaultContainer = normalizePath defaultContainer

    do StorePath.Validate defaultContainer

    /// <summary>
    ///     Creates an Azure blob store based CloudFileStore instance that connects to provided storage account.
    /// </summary>
    /// <param name="account">Azure storage account.</param>
    /// <param name="defaultContainer">Default container to be used be store instance.</param>
    static member Create(account : AzureStorageAccount, ?defaultContainer : string) = 
        ignore account.ConnectionString // force check that connection string is present in current host.
        new BlobStore(account, defaultArg defaultContainer "")

    /// <summary>
    ///     Creates an Azure blob store based CloudFileStore instance that connects to provided connection string.
    /// </summary>
    /// <param name="connectionString">Azure storage account connection string.</param>
    /// <param name="defaultContainer">Default container to be used be store instance.</param>
    static member Create(connectionString : string, ?defaultContainer : string) = 
        BlobStore.Create(AzureStorageAccount.Parse connectionString, ?defaultContainer = defaultContainer)

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
                let blobs = new ResizeArray<string>()
                let rec aux (token : BlobContinuationToken) = async {
                    let! (result : BlobResultSegment) = 
                        match path.SubDirectory with
                        | None -> containerRef.ListBlobsSegmentedAsync(token)
                        | Some prefix -> containerRef.ListBlobsSegmentedAsync(prefix, token)
                    for blob in result.Results do
                        match blob with
                        | :? ICloudBlob as icb -> blobs.Add(icb.Name)
                        | _ -> ()
                    if result.ContinuationToken = null then return ()
                    else return! aux result.ContinuationToken
                }
                do! aux null
                return 
                    this.Combine(container, blobs)
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
            | { Container = Root; SubDirectory = _ } -> failwith "Deleting root directory not supported."
            | { Container = c; SubDirectory = None } ->
                let! container = getContainerReference account c
                let! _ = container.DeleteIfExistsAsync()
                return ()
            | { Container = c; SubDirectory = Some s } ->
                let! container = getContainerReference account c
                let sub = container.GetDirectoryReference(s)
                let blobs = sub.ListBlobs(true)
                do! blobs |> Seq.map (fun b -> async {
                        let p = b.Uri.Segments |> Array.last
                        let! blob = getBlobReference account (Path.Combine(container.Name,p))
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
                    let client = account.BlobClient
                    return client.ListContainers() 
                            |> Seq.map (fun c -> normalizePath c.Name)
                            |> Seq.toArray
                | { Container = _; SubDirectory = _ } -> return! Async.Raise <| NotImplementedException()
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

        member this.CopyOfStream(source: Stream, path: string) : Async<unit> = async {
            let path = normalizePath path
            let! blob = getBlobReference account path
            let options = BlobRequestOptions(ServerTimeout = Nullable<_>(TimeSpan.FromMinutes(40.)))
            do! blob.UploadFromStreamAsync(source, null, options, OperationContext()).ContinueWith ignore
        }
        
        member this.CopyToStream(path: string, target: Stream) : Async<unit> = async {
            try
                let path = normalizePath path
                let! blob = getBlobReference account path
                do! blob.DownloadToStreamAsync(target).ContinueWith ignore
            with e when StoreException.NotFound e ->
                return raise <| new FileNotFoundException(path, e)
        }