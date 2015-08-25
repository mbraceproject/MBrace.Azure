namespace MBrace.Azure.Runtime.Utilities

open System
open System.IO
open System.Text.RegularExpressions
open System.Threading.Tasks

open Microsoft.WindowsAzure.Storage.Table

[<AutoOpen>]
module Utils =

    let guid() = Guid.NewGuid().ToString()

    let toGuid guid = Guid.Parse(guid)
    let fromGuid(guid : Guid) = guid.ToString()

    let uri fmt = Printf.ksprintf (fun s -> new Uri(s)) fmt

    let inline nullable< 'T when 'T : struct and 'T : (new : unit -> 'T) and  'T :> ValueType > (value : 'T) = 
        new Nullable<'T>(value)

    let inline nullableDefault< 'T when 'T : struct and 'T : (new : unit -> 'T) and  'T :> ValueType > = 
        new Nullable<'T>()

    let (|Null|Nullable|) (value : Nullable<'T>) =
        if value.HasValue then Nullable(value.Value) else Null

    type Async with
        static member Sleep(time : TimeSpan) = Async.Sleep(int time.TotalMilliseconds)
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

    [<RequireQualifiedAccess>]
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


open MBrace.Core.Internals

[<Sealed; AutoSerializable(false)>]
type Live<'T>(provider : unit -> Async<'T>, initial : Choice<'T,exn>, ?keepLast : bool, ?interval : int, ?stopf : Choice<'T, exn> -> bool) =
    let interval = defaultArg interval 500
    let keepLast = defaultArg keepLast false
    let stopf = defaultArg stopf (fun _ -> false)
    let mutable stop = false
    let mutable value = initial

    let runOnce () = async {
        let! choice = Async.Catch <| provider ()
        match choice with
        | Choice1Of2 _ as v -> value <- v
        | Choice2Of2 _ when keepLast -> ()
        | Choice2Of2 _ as v -> value <- v
        return choice
    }

    let rec update () = async {
        let! choice = runOnce()
        if stopf choice || stop then 
            return ()
        else
            do! Async.Sleep interval
            return! update ()
    }

    do 
        runOnce() |> Async.Ignore |> Async.RunSync
        update () |> Async.Start

    member __.TryGetValue () = 
        match value with
        | Choice1Of2 v -> Some v
        | Choice2Of2 _ -> None

    member __.Value =
        match value with
        | Choice1Of2 v -> v
        | Choice2Of2 e -> raise e

    member __.Stop () = stop <- true