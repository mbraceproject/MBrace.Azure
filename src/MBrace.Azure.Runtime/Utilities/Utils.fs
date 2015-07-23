namespace MBrace.Azure.Runtime.Utilities

    open System
    open System.IO
    open System.Text.RegularExpressions
    open System.Threading.Tasks
    open Microsoft.WindowsAzure.Storage.Table

    [<AutoOpen>]
    module Utils =

        let guid() = Guid.NewGuid().ToString("N")

        let toGuid guid = Guid.Parse(guid)
        let fromGuid(guid : Guid) = guid.ToString("N")

        let uri fmt = Printf.ksprintf (fun s -> new Uri(s)) fmt

        let inline nullable< 'T when 'T : struct and 'T : (new : unit -> 'T) and  'T :> ValueType > (value : 'T) = 
            new Nullable<'T>(value)

        let inline nullableDefault< 'T when 'T : struct and 'T : (new : unit -> 'T) and  'T :> ValueType > = 
            new Nullable<'T>()

        let (|Null|Nullable|) (value : Nullable<'T>) =
            if value.HasValue then Nullable(value.Value) else Null

        /// generates a human readable string for byte sizes
        /// including a KiB, MiB, GiB or TiB suffix depending on size
        let getHumanReadableByteSize (size : int64) =
            if size <= 512L then sprintf "%d bytes" size
            elif size <= 512L * 1024L then sprintf "%.2f KiB" (decimal size / decimal 1024L)
            elif size <= 512L * 1024L * 1024L then sprintf "%.2f MiB" (decimal size / decimal (1024L * 1024L))
            elif size <= 512L * 1024L * 1024L * 1024L then sprintf "%.2f GiB" (decimal size / decimal (1024L * 1024L * 1024L))
            else sprintf "%.2f TiB" (decimal size / decimal (1024L * 1024L * 1024L * 1024L))

        type Async with
            static member Cast<'U>(task : Async<obj>) = async { let! t = task in return box t :?> 'U }
            static member Sleep(timespan : TimeSpan) = Async.Sleep(int timespan.TotalMilliseconds)
            static member AwaitTask(task : Task) = Async.AwaitTask(task.ContinueWith ignore)
            static member TrapExc<'T, 'Exc when 'Exc :> exn>(task : Async<'T>) =
                async {
                    let! result = Async.Catch task
                    match result with
                    | Choice1Of2 r -> return r
                    | Choice2Of2 e when (e :? 'Exc) -> return! Async.TrapExc task
                    | Choice2Of2 e -> return raise e
                }

        type AsyncBuilder with
            member __.Bind(f : Task<'T>, g : 'T -> Async<'S>) : Async<'S> = 
                __.Bind(Async.AwaitTask f, g)
            member __.Bind(f : Task, g : unit -> Async<'S>) : Async<'S> =
                __.Bind(Async.AwaitTask(f.ContinueWith ignore), g)
            member __.ReturnFrom(f : Task<'T>) : Async<'T> =
                __.ReturnFrom(Async.AwaitTask f)
            member __.ReturnFrom(f : Task) : Async<unit> =
                __.ReturnFrom(Async.AwaitTask f)


    open MBrace.Core.Internals

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

        do runOnce() |> Async.Ignore |> Async.RunSync
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
            


    [<RequireQualifiedAccess>]
    module Convert =
        
        open System.Text
        open System.IO
        open System.Collections.Generic

        // taken from : http://www.atrevido.net/blog/PermaLink.aspx?guid=debdd47c-9d15-4a2f-a796-99b0449aa8af
        let private encodingIndex = "qaz2wsx3edc4rfv5tgb6yhn7ujm8k9lp"
        let private inverseIndex = encodingIndex |> Seq.mapi (fun i c -> c,i) |> dict

        /// convert bytes to base-32 string: useful for file names in case-insensitive file systems
        let toBase32String(bytes : byte []) =
            let b = new StringBuilder()
            let mutable hi = 5
            let mutable idx = 0uy
            let mutable i = 0
                
            while i < bytes.Length do
                // do we need to use the next byte?
                if hi > 8 then
                    // get the last piece from the current byte, shift it to the right
                    // and increment the byte counter
                    idx <- bytes.[i] >>> (hi - 5)
                    i <- i + 1
                    if i <> bytes.Length then
                        // if we are not at the end, get the first piece from
                        // the next byte, clear it and shift it to the left
                        idx <- ((bytes.[i] <<< (16 - hi)) >>> 3) ||| idx

                    hi <- hi - 3
                elif hi = 8 then
                    idx <- bytes.[i] >>> 3
                    i <- i + 1
                    hi <- hi - 3
                else
                    // simply get the stuff from the current byte
                    idx <- (bytes.[i] <<< (8 - hi)) >>> 3
                    hi <- hi + 5

                b.Append (encodingIndex.[int idx]) |> ignore

            b.ToString ()