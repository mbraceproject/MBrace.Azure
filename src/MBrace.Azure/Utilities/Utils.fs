namespace MBrace.Azure.Runtime.Utilities

open System
open System.IO
open System.Text.RegularExpressions
open System.Threading.Tasks

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
            with :? AggregateException as ae when ae.InnerExceptions.Count = 1 -> 
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
            let chunks = ResizeArray<'T []> ()
            let builder = ResizeArray<'T> ()
            for i = 0 to ts.Length - 1 do
                if builder.Count = chunkSize then
                    chunks.Add (builder.ToArray())
                    builder.Clear()

                builder.Add ts.[i]

            if builder.Count > 0 then
                chunks.Add(builder.ToArray())

            chunks.ToArray()

        let last (ts : 'T []) = ts.[ts.Length - 1]


    [<RequireQualifiedAccess>]
    module Seq =
        /// <summary>
        ///      Splits input sequence into chunks of at most chunkSize.
        /// </summary>
        /// <param name="chunkSize">Maximal size used by chunks</param>
        /// <param name="ts">Input array.</param>
        let chunksOf chunkSize (ts : seq<'T>) : 'T [][] =
            if chunkSize <= 0 then invalidArg "chunkSize" "must be positive integer."
            let chunks = ResizeArray<'T []> ()
            let builder = ResizeArray<'T> ()
            use e = ts.GetEnumerator()
            while e.MoveNext() do
                if builder.Count = chunkSize then
                    chunks.Add (builder.ToArray())
                    builder.Clear()

                builder.Add e.Current

            if builder.Count > 0 then
                chunks.Add(builder.ToArray())

            chunks.ToArray()