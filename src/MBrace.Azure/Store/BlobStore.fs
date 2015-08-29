namespace MBrace.Azure.Store

open System
open System.IO
open System.Security.AccessControl
open System.Runtime.Serialization

open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Blob

open MBrace.Core.Internals
open MBrace.Azure.Runtime.Utilities
open MBrace.Azure.Store.TableEntities.Table

///  MBrace File Store implementation that uses Azure Blob Storage as backend.
[<Sealed; DataContract>]
type BlobStore private (connectionString : string, defaultContainer : string) =
    // TODO: validate defaultContainer input

    [<DataMember(Name = "ConnectionString")>]
    let connectionString = connectionString

    [<DataMember(Name = "DefaultContainer")>]
    let defaultContainer = defaultContainer

    [<IgnoreDataMember>]
    let mutable acc = CloudStorageAccount.Parse(connectionString)

    [<OnDeserialized>]
    let _onDeserialized (_ : StreamingContext) =
        acc <- CloudStorageAccount.Parse(connectionString)

    /// <summary>
    ///     Creates an MBrace blob storage interface that connects to storage account with provided connection string.
    /// </summary>
    /// <param name="connectionString">Azure storage account connection string.</param>
    /// <param name="defaultContainer">Default container to be used be store instance.</param>
    static member Create(connectionString : string, ?defaultContainer : string) = new BlobStore(connectionString, defaultArg defaultContainer "")

    interface ICloudFileStore with
        member this.BeginWrite(path: string): Async<Stream> = 
            async {
                let! blob = getBlobReference acc path
                let! stream = blob.OpenWriteAsync()
                return stream :> Stream
            }
        
        member this.ReadETag(path: string, etag: ETag): Async<Stream option> = 
            async {
                let! blob = getBlobReference acc path
                let! stream = async { return! blob.OpenReadAsync(AccessCondition.GenerateIfMatchCondition(etag), BlobRequestOptions(), null) } |> Async.Catch
                match stream with
                | Choice1Of2 s -> 
                    return Some s
                | Choice2Of2 e when PreconditionFailed e -> 
                    return None
                | Choice2Of2 e when NotFound e ->
                    return raise <| new FileNotFoundException(path, e)
                | Choice2Of2 e -> return raise e
            }

        member this.TryGetETag(path: string): Async<ETag option> = 
            async {
                let! blob = getBlobReference acc path
                try
                    do! blob.FetchAttributesAsync()
                    if String.IsNullOrEmpty blob.Properties.ETag then 
                        return None
                    else
                        return Some blob.Properties.ETag
                with
                | ex when NotFound ex -> return None
                | ex -> return raise ex
            }
        
        member this.Name = "MBrace.Azure.Store.BlobStore"
        member this.Id : string = acc.BlobStorageUri.PrimaryUri.ToString()

        member this.DefaultDirectory = defaultContainer
        member this.WithDefaultDirectory container = new BlobStore(connectionString, container) :> _

        member this.GetRootDirectory () = String.Empty

        member this.GetRandomDirectoryName() : string = Guid.NewGuid().ToString()

        member this.IsPathRooted(path : string) = 
            let dir = StoreDirectory.Parse path
            dir.Container <> Root

        member this.GetDirectoryName(path : string) = Path.GetDirectoryName(path)

        member this.GetFileName(path : string) = Path.GetFileName(path)

        member this.Combine(paths : string []) : string = 
            Path.Combine(paths)

        member this.GetFileSize(path: string) : Async<int64> = 
            async {
                let! blob = getBlobReference acc path
                let! result = Async.Catch <| Async.AwaitTaskCorrect(blob.FetchAttributesAsync())
                match result with
                | Choice1Of2 () when blob.Properties.Length = -1L -> return! Async.Raise <| FileNotFoundException(path)
                | Choice1Of2 () -> return blob.Properties.Length
                | Choice2Of2 ex when NotFound ex -> return! Async.Raise <| FileNotFoundException(path)
                | Choice2Of2 ex -> return! Async.Raise ex
            }

        member this.FileExists(path: string) : Async<bool> = 
            async {
                let storePath = StorePath.Parse path
                let container = getContainerReference acc storePath.Container

                let! b1 = container.ExistsAsync()
                if b1 then
                    let! blob = getBlobReference acc path
                    return! blob.ExistsAsync()
                else 
                    return false
            }

        member this.EnumerateFiles(container : string) : Async<string []> = 
            async {
                try
                    let path = StoreDirectory.Parse container
                    let containerRef = getContainerReference acc path.Container
                    let blobs = new ResizeArray<string>()
                    let rec aux (token : BlobContinuationToken) = async {
                        let! (result : BlobResultSegment) = 
                            match path.SubDirectory with
                            | None -> containerRef.ListBlobsSegmentedAsync(token)
                            | Some prefix -> containerRef.ListBlobsSegmentedAsync(prefix, token)
                        for blob in result.Results do
                            let p = blob.Uri.Segments |> Array.last
                            blobs.Add(Path.Combine(container, p))
                        if result.ContinuationToken = null then return ()
                        else return! aux result.ContinuationToken
                    }
                    do! aux null
                    return blobs.ToArray()

                with e when NotFound e ->
                    return raise <| new DirectoryNotFoundException(container, e)
            }
        
        member this.DeleteFile(path: string) : Async<unit> = 
            async {
                try
                    let! blob = getBlobReference acc path
                    do! blob.DeleteAsync()
                with e when NotFound e ->
                    return raise <| new FileNotFoundException(path, e)
            }

        member this.DirectoryExists(container: string) : Async<bool> = 
            async {
                let path = StoreDirectory.Parse container
                match path.Container with
                | Container _ as c ->
                    let container = getContainerReference acc c
                    return! container.ExistsAsync()
                | Root ->
                    return true
            }
        
        member this.CreateDirectory(container: string) : Async<unit> = 
            async {
                let path = StoreDirectory.Parse container
                match path.Container with
                | Container _ as c ->
                    let container = getContainerReference acc c
                    let! _ =  container.CreateIfNotExistsAsync()
                    return ()
                | Root -> 
                    return ()
            }

        member this.DeleteDirectory(container: string, recursiveDelete : bool) : Async<unit> = 
            async {
                ignore recursiveDelete
                let path = StoreDirectory.Parse container
                match path with
                | { Container = Root; SubDirectory = _ } -> failwith "Deleting root directory not supported."
                | { Container = c; SubDirectory = None } ->
                    let container = getContainerReference acc c
                    let! _ = container.DeleteIfExistsAsync()
                    return ()
                | { Container = c; SubDirectory = Some s } ->
                    let container = getContainerReference acc c
                    let sub = container.GetDirectoryReference(s)
                    let blobs = sub.ListBlobs(true)
                    do! blobs |> Seq.map (fun b -> async {
                            let p = b.Uri.Segments |> Array.last
                            let! blob = getBlobReference acc (Path.Combine(container.Name,p))
                            let! _ = blob.DeleteIfExistsAsync()
                            ()
                        })
                        |> Async.Parallel
                        |> Async.Ignore
            }
        
        member this.EnumerateDirectories(directory : string) : Async<string []> = 
            async {
                try
                    let path = StoreDirectory.Parse directory
                    match path with
                    | { Container = Root; SubDirectory = _ } ->
                        let client = getBlobClient acc
                        return client.ListContainers() 
                               |> Seq.map (fun c -> c.Name)
                               |> Seq.toArray
                    | { Container = _; SubDirectory = _ } -> return! Async.Raise <| NotImplementedException()
                with e when NotFound e ->
                    return raise <| new DirectoryNotFoundException(directory, e)
            }

        member this.WriteETag(path: string, writer : Stream -> Async<'R>) : Async<ETag * 'R> = 
            async {
                let! blob = getBlobReference acc path
                // http://msdn.microsoft.com/en-us/library/azure/dd179431.aspx
                let! result = async {
                    let options = BlobRequestOptions(ServerTimeout = Nullable<_>(TimeSpan.FromMinutes(40.)))
                    use! stream = blob.OpenWriteAsync(null, options, OperationContext())
                    return! writer(stream)
                }
                return blob.Properties.ETag, result
            } 
        
        member this.BeginRead(path: string) : Async<Stream> = 
            async {
                try
                    let! blob = getBlobReference acc path
                    return! blob.OpenReadAsync()
                with e when NotFound e ->
                    return raise <| new FileNotFoundException(path, e)
            }

        member this.CopyOfStream(source: Stream, target: string) : Async<unit> = 
            async {
                let! blob = getBlobReference acc target
                let options = BlobRequestOptions(ServerTimeout = Nullable<_>(TimeSpan.FromMinutes(40.)))
                do! blob.UploadFromStreamAsync(source, null, options, OperationContext()).ContinueWith ignore
            }
        
        member this.CopyToStream(sourceFile: string, target: Stream) : Async<unit> = 
            async {
                try
                    let! blob = getBlobReference acc sourceFile
                    do! blob.DownloadToStreamAsync(target).ContinueWith ignore
                with e when NotFound e ->
                    return raise <| new FileNotFoundException(sourceFile, e)
            }