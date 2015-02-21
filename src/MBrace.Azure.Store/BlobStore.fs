namespace MBrace.Azure.Store

open System
open System.IO
open System.Security.AccessControl
open System.Runtime.Serialization

open MBrace.Store
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Blob

///  Store implementation that uses a Azure Blob Storage as backend.
[<Sealed; DataContract>]
type BlobStore private (connectionString : string) =

    [<DataMember(Name = "ConnectionString")>]
    let connectionString = connectionString

    [<IgnoreDataMember>]
    let mutable acc = CloudStorageAccount.Parse(connectionString)

    [<OnDeserialized>]
    let _onDeserialized (_ : StreamingContext) =
        acc <- CloudStorageAccount.Parse(connectionString)

    ///  Store implementation that uses a Azure Blob Storage as backend.
    static member Create(connectionString : string) = new BlobStore(connectionString)

    interface ICloudFileStore with
        member this.Name = "MBrace.Azure.Store.BlobStore"
        member this.Id : string = acc.BlobStorageUri.PrimaryUri.ToString()

        member this.GetRootDirectory () = String.Empty

        member this.GetRandomDirectoryName() : string = Guid.NewGuid().ToString()

        member this.TryGetFullPath(path : string) = Some path

        member this.GetDirectoryName(path : string) = Path.GetDirectoryName(path)

        member this.GetFileName(path : string) = Path.GetFileName(path)

        member this.Combine(paths : string []) : string = 
            Path.Combine(paths)

        member this.GetFileSize(path: string) : Async<int64> = 
            async {
                let! blob = getBlobRef acc path
                let! _ = Async.AwaitIAsyncResult <| blob.FetchAttributesAsync()
                return blob.Properties.Length
            }
        member this.FileExists(path: string) : Async<bool> = 
            async {
                let directory, file = splitPath path
                let container = getContainer acc directory
                
                let! b1 = Async.AwaitTask(container.ExistsAsync())
                if b1 then
                    let blob = container.GetBlockBlobReference(file)
                    return! Async.AwaitTask(blob.ExistsAsync())
                else 
                    return false
            }

        member this.EnumerateFiles(container : string) : Async<string []> = 
            async {
                let containerRef = getContainer acc container
                let blobs = new ResizeArray<string>()
                let rec aux (token : BlobContinuationToken) = async {
                    let! (result : BlobResultSegment) = containerRef.ListBlobsSegmentedAsync(token)
                    for blob in result.Results do
                        let p = blob.Uri.Segments |> Seq.last
                        blobs.Add(Path.Combine(container, p))
                    if result.ContinuationToken = null then return ()
                    else return! aux result.ContinuationToken
                }
                do! aux null
                return blobs.ToArray()
            }
        
        member this.DeleteFile(path: string) : Async<unit> = 
            async {
                let! blob = getBlobRef acc path
                let! _ =  Async.AwaitIAsyncResult <| blob.DeleteAsync()
                return ()
            }

        member this.DirectoryExists(container: string) : Async<bool> = 
            async {
                let container = getContainer acc container
                return! Async.AwaitTask <| container.ExistsAsync()
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
        
        member this.EnumerateDirectories(directory) : Async<string []> = 
            async {
                let client = getBlobClient acc
                return client.ListContainers(directory) 
                       |> Seq.map (fun c -> c.Name)
                       |> Seq.toArray
            }

        member this.Write(path: string, writer : Stream -> Async<'R>) : Async<'R> = 
            async {
                let! blob = getBlobRef acc path
                // http://msdn.microsoft.com/en-us/library/azure/dd179431.aspx
                let options = BlobRequestOptions(ServerTimeout = Nullable<_>(TimeSpan.FromMinutes(10.)))
                use! stream = blob.OpenWriteAsync(null, options, OperationContext())
                return! writer(stream)
            } 
        
        member this.BeginRead(path: string) : Async<Stream> = 
            async {
                let! blob = getBlobRef acc path
                return! Async.AwaitTask(blob.OpenReadAsync())
            }

        member this.OfStream(source: Stream, target: string) : Async<unit> = 
            async {
                let! blob = getBlobRef acc target
                let options = BlobRequestOptions(ServerTimeout = Nullable<_>(TimeSpan.FromMinutes(10.)))
                let! _ = Async.AwaitIAsyncResult <| blob.UploadFromStreamAsync(source, null, options, OperationContext())
                return ()
            }
        
        member this.ToStream(sourceFile: string, target: Stream) : Async<unit> = 
            async {
                let! blob = getBlobRef acc sourceFile
                let! _ = Async.AwaitIAsyncResult <| blob.DownloadToStreamAsync(target)
                return ()
            }