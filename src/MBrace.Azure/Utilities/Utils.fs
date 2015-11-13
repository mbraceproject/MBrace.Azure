namespace MBrace.Azure.Runtime.Utilities

open System
open System.IO
open System.Text.RegularExpressions
open System.Threading.Tasks

[<AutoOpen>]
module Utils =

    type internal OAttribute = System.Runtime.InteropServices.OptionalAttribute
    type internal DAttribute = System.Runtime.InteropServices.DefaultParameterValueAttribute

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
        static member Cast<'U>(task : Async<obj>) = async { let! t = task in return box t :?> 'U }

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