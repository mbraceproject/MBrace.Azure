namespace MBrace.Azure.Store

open System
open System.IO
open System.Threading.Tasks

open Microsoft.WindowsAzure.Storage

[<AutoOpen>]
module internal Utils =
    open MBrace.Store

    /// Generates a new guid in string form
    let guid() = Guid.NewGuid().ToString("N")
    /// Uri formatter function
    let uri fmt = Printf.ksprintf (fun s -> new Uri(s)) fmt

    module Array =
        /// <summary>
        ///      Splits input array into chunks of at most chunkSize.
        /// </summary>
        /// <param name="chunkSize">Maximal size used by chunks</param>
        /// <param name="ts">Input array.</param>
        let chunksOf chunkSize (ts : 'T []) =
            if chunkSize <= 0 then invalidArg "chunkSize" "must be positive integer."
            let N = ts.Length
            let chunkCount = (float N) / (float chunkSize) |> ceil |> int
            let chunks = ResizeArray<'T []> ()
            for i = 0  to chunkCount - 1 do
                let s, e = N * i / chunkCount, N * (i + 1) / chunkCount
                chunks.Add <| Array.sub ts s (e - s)
            chunks.ToArray()

    type Async with
        static member Cast<'U>(task : Async<obj>) = async { let! t = task in return box t :?> 'U }
        static member Sleep(timespan : TimeSpan) = Async.Sleep(int timespan.TotalMilliseconds)
        static member AwaitTask(task : Task) = Async.AwaitTask(task.ContinueWith ignore)

    type AsyncBuilder with
        member __.Bind(f : Task<'T>, g : 'T -> Async<'S>) : Async<'S> = 
            __.Bind(Async.AwaitTask f, g)
        member __.Bind(f : Task, g : unit -> Async<'S>) : Async<'S> =
            __.Bind(Async.AwaitTask(f.ContinueWith ignore), g)
        member __.ReturnFrom(f : Task<'T>) : Async<'T> =
            __.ReturnFrom(Async.AwaitTask f)
        member __.ReturnFrom(f : Task) : Async<unit> =
            __.ReturnFrom(Async.AwaitTask f)

    /// Creates a Microsoft.WindowsAzure.Storage.BlobClient instance given a cloud storage account
    let getBlobClient (account : CloudStorageAccount) =
        let client = account.CreateCloudBlobClient()
            
        client.DefaultRequestOptions.ParallelOperationThreadCount <- System.Nullable(4 * System.Environment.ProcessorCount)
        client.DefaultRequestOptions.SingleBlobUploadThresholdInBytes <- System.Nullable(1L <<< 23) // 8MB, possible ranges: 1..64MB, default 32MB
        
        client

    /// <summary>
    ///     Creates a blob storage container reference given account and container name
    /// </summary>
    /// <param name="account">Storage account instance.</param>
    /// <param name="container">Container name</param>
    let getContainer (account : CloudStorageAccount) (container : string) = 
        let client = getBlobClient account
        client.GetContainerReference container

    /// <summary>
    ///     Creates a blob reference given account and full path.
    /// </summary>
    /// <param name="account">Cloud storage account.</param>
    /// <param name="path">Path to blob.</param>
    let getBlobRef account path = async {
        let container, blob = Path.GetDirectoryName path, Path.GetFileName path
        let container = getContainer account container
        let! _ = container.CreateIfNotExistsAsync()
        return container.GetBlockBlobReference(blob)
    }