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
        member inline __.Bind(f : Task<'T>, g : 'T -> Async<'S>) : Async<'S> = 
            __.Bind(Async.AwaitTask f, g)
        member inline __.Bind(f : Task, g : unit -> Async<'S>) : Async<'S> =
            __.Bind(Async.AwaitTask(f.ContinueWith ignore), g)
        member inline __.ReturnFrom(f : Task<'T>) : Async<'T> =
            __.ReturnFrom(Async.AwaitTask f)
        member inline __.ReturnFrom(f : Task) : Async<unit> =
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