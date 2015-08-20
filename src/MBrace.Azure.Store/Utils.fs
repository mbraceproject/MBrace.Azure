namespace MBrace.Azure.Store

open System
open System.IO
open System.Threading.Tasks

open Microsoft.WindowsAzure.Storage

[<AutoOpen>]
module internal Utils =

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

        let last (ts : 'T []) = ts.[ts.Length - 1]

    type Async with
        static member Raise(e : exn) = Async.FromContinuations(fun (_,ec,_) -> ec e)
        static member Cast<'U>(task : Async<obj>) = async { let! t = task in return box t :?> 'U }

        /// <summary>
        ///     Asynchronously awaits a task in a way that correctly exposes user exceptions.
        /// </summary>
        /// <param name="task">Task to be awaited.</param>
        static member AwaitTaskCorrect(task : Task<'T>) = async {
            try return! Async.AwaitTask task
            with :? AggregateException as ae -> 
                return! Async.Raise (ae.InnerExceptions.[0])
        }

        /// <summary>
        ///     Asynchronously awaits a task in a way that correctly exposes user exceptions.
        /// </summary>
        /// <param name="task">Task to be awaited.</param>
        static member AwaitTaskCorrect(task : Task) =
            Async.AwaitTaskCorrect(task.ContinueWith ignore)

    type AsyncBuilder with
        member inline __.Bind(f : Task<'T>, g : 'T -> Async<'S>) : Async<'S> = 
            __.Bind(Async.AwaitTaskCorrect f, g)
        member inline __.Bind(f : Task, g : unit -> Async<'S>) : Async<'S> =
            __.Bind(Async.AwaitTaskCorrect (f.ContinueWith ignore), g)
        member inline __.ReturnFrom(f : Task<'T>) : Async<'T> =
            __.ReturnFrom(Async.AwaitTaskCorrect f)
        member inline __.ReturnFrom(f : Task) : Async<unit> =
            __.ReturnFrom(Async.AwaitTaskCorrect (f.ContinueWith ignore))

    let validateContainerName =
        //https://msdn.microsoft.com/en-us/library/azure/dd135715.aspx
        let letters = set {'a'..'z'}
        let nums = set { '0'..'9' }
        let valid = letters + nums + Set.singleton '-'
        fun (container : string) ->
            let isValid =
                container.Length >= 3 
                && container.Length <= 63
                && container |> Seq.forall valid.Contains
                && container |> Seq.head <> '-'
            if not isValid then failwithf "Invalid container '%s'" container
            

    /// Creates a Microsoft.WindowsAzure.Storage.BlobClient instance given a cloud storage account
    let getBlobClient (account : CloudStorageAccount) =
        let client = account.CreateCloudBlobClient()
        client.DefaultRequestOptions.ServerTimeout <- Nullable(TimeSpan.FromMinutes(40.))
        client.DefaultRequestOptions.ParallelOperationThreadCount <- System.Nullable(min 64 <| 4 * System.Environment.ProcessorCount)
        client.DefaultRequestOptions.SingleBlobUploadThresholdInBytes <- System.Nullable(4L * 1024L * 1024L) // possible ranges: 1..64MB, default 32MB
        
        client

    /// <summary>
    ///     Creates a blob storage container reference given account and container name
    /// </summary>
    /// <param name="account">Storage account instance.</param>
    /// <param name="container">Container name</param>
    let getContainer (account : CloudStorageAccount) (container : string) = 
        validateContainerName container
        let client = getBlobClient account
        client.GetContainerReference container

    /// <summary>
    ///     Creates a blob reference given account and full path.
    /// </summary>
    /// <param name="account">Cloud storage account.</param>
    /// <param name="path">Path to blob.</param>
    let getBlobRef account (path : string) = async {
        let container, blob = 
            match path.Split([|'/'; '\\'|], 2) with
            | [|c; b |] -> c, b
            | [|_|] -> failwithf "Invalid path '%s'. Top level files not allowed." path
            | _ -> failwithf "Invalid path '%s'." path
        let container = getContainer account container
        let! _ = container.CreateIfNotExistsAsync()
        return container.GetBlockBlobReference(blob)
    }