namespace MBrace.Azure.Store

open System
open System.IO
open System.Security.AccessControl
open System.Runtime.Serialization

open MBrace.Core.Internals

open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Blob
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
                let! blob = getBlobRef acc path
                let! stream = blob.OpenWriteAsync()
                return stream :> Stream
            }
        
        member this.ReadETag(path: string, etag: ETag): Async<Stream option> = 
            async {
                let! blob = getBlobRef acc path
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
                let! blob = getBlobRef acc path
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

        member this.IsPathRooted(path : string) = path.Contains("/")

        member this.GetDirectoryName(path : string) = Path.GetDirectoryName(path)

        member this.GetFileName(path : string) = Path.GetFileName(path)

        member this.Combine(paths : string []) : string = 
            Path.Combine(paths)

        member this.GetFileSize(path: string) : Async<int64> = 
            async {
                let! blob = getBlobRef acc path
                do! blob.FetchAttributesAsync()
                return
                    if blob.Properties.Length = -1L then
                        raise <| new FileNotFoundException(path)
                    else
                        blob.Properties.Length
            }

        member this.FileExists(path: string) : Async<bool> = 
            async {
                let directory = Path.GetDirectoryName path 
                let file = Path.GetFileName path
                let container = getContainer acc directory
                
                let! b1 = container.ExistsAsync()
                if b1 then
                    let blob = container.GetBlockBlobReference(file)
                    return! blob.ExistsAsync()
                else 
                    return false
            }

        member this.EnumerateFiles(container : string) : Async<string []> = 
            async {
                try
                    let containerRef = getContainer acc container
                    let blobs = new ResizeArray<string>()
                    let rec aux (token : BlobContinuationToken) = async {
                        let! (result : BlobResultSegment) = containerRef.ListBlobsSegmentedAsync(token)
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
                let! blob = getBlobRef acc path
                do! blob.DeleteAsync().ContinueWith ignore
                return ()
            }

        member this.DirectoryExists(container: string) : Async<bool> = 
            async {
                let container = getContainer acc container
                return! container.ExistsAsync()
            }
        
        member this.CreateDirectory(container: string) : Async<unit> = 
            async {
                let container = getContainer acc container
                let! _ =  container.CreateIfNotExistsAsync()
                return ()
            }

        member this.DeleteDirectory(container: string, recursiveDelete : bool) : Async<unit> = 
            async {
                ignore recursiveDelete
                let container = getContainer acc container
                let! _ = container.DeleteIfExistsAsync()
                return ()
            }
        
        member this.EnumerateDirectories(directory : string) : Async<string []> = 
            async {
                try
                    let client = getBlobClient acc
                    return client.ListContainers(directory) 
                           |> Seq.map (fun c -> c.Name)
                           |> Seq.toArray

                with e when NotFound e ->
                    return raise <| new DirectoryNotFoundException(directory, e)
            }

        member this.WriteETag(path: string, writer : Stream -> Async<'R>) : Async<ETag * 'R> = 
            async {
                let! blob = getBlobRef acc path
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
                    let! blob = getBlobRef acc path
                    return! blob.OpenReadAsync()
                with e when NotFound e ->
                    return raise <| new FileNotFoundException(path, e)
            }

        member this.CopyOfStream(source: Stream, target: string) : Async<unit> = 
            async {
                let! blob = getBlobRef acc target
                let options = BlobRequestOptions(ServerTimeout = Nullable<_>(TimeSpan.FromMinutes(40.)))
                do! blob.UploadFromStreamAsync(source, null, options, OperationContext()).ContinueWith ignore
            }
        
        member this.CopyToStream(sourceFile: string, target: Stream) : Async<unit> = 
            async {
                try
                    let! blob = getBlobRef acc sourceFile
                    do! blob.DownloadToStreamAsync(target).ContinueWith ignore
                with e when NotFound e ->
                    return raise <| new FileNotFoundException(sourceFile, e)
            }