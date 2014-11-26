namespace Nessos.MBrace.Azure.Runtime.Common

    open System
    open System.Threading.Tasks
    open Microsoft.WindowsAzure.Storage.Table

    [<AutoOpen>]
    module Utils =

        let guid() = Guid.NewGuid().ToString("N")
        let uri fmt = Printf.ksprintf (fun s -> new Uri(s)) fmt
    
        let inline ofTask (t : Task) : Task<unit> = t.ContinueWith ignore

        type Async with
            static member inline Cast<'U>(task : Async<obj>) = async { let! t = task in return box t :?> 'U }

        type AsyncBuilder with
            member inline __.Bind(f : Task<'T>, g : 'T -> Async<'S>) : Async<'S> = 
                async { let! r = Async.AwaitTask(f) in return! g r }

        type Uri with
            member u.ResourceId = u.Scheme
            member u.PartitionWithScheme = sprintf "%s:%s" u.Scheme u.PartitionKey
            member u.FileWithScheme = sprintf "%s:%s" u.Scheme u.File

            // Primary
            member u.Container = 
                let s = u.Segments.[0] in if s.EndsWith("/") then s.Substring(0, s.Length-1) else s
            member u.Table = u.Container
            member u.Queue = u.Container
        
            // Secondary
            member u.File = u.Segments.[1]
            member u.PartitionKey = u.File

            // Unique
            member u.RowKey = u.Segments.[2]

    module Storage =
        open Nessos.MBrace.Azure.Runtime

        let processIdToStorageId (pid : string) = 
            sprintf "process%s" <| Guid.Parse(pid).ToString("N").Substring(0,7) // TODO : change

    type Live<'T>(provider : unit -> Async<'T>, initial : Choice<'T,exn>, ?keepLast : bool, ?interval : int, ?stopf : Choice<'T, exn> -> bool) =
        let interval = defaultArg interval 500
        let keepLast = defaultArg keepLast false
        let stopf = defaultArg stopf (fun _ -> false)
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
            if stopf choice then 
                return ()
            else
                do! Async.Sleep interval
                return! update ()
        }

        do runOnce() |> Async.Ignore |> Async.RunSynchronously
           update () |> Async.Start

        member __.TryGetValue () = 
            match value with
            | Choice1Of2 v -> Some v
            | Choice2Of2 _ -> None

        member __.Value =
            match value with
            | Choice1Of2 v -> v
            | Choice2Of2 e -> raise e