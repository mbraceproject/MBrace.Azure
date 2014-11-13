[<AutoOpen>]
module Nessos.MBrace.Azure.Runtime.Utils

    open System
    open System.Threading.Tasks
    open Microsoft.WindowsAzure.Storage.Table

    let guid() = Guid.NewGuid().ToString("N")
    let uri fmt = Printf.ksprintf (fun s -> new Uri(s)) fmt
    
    let inline ofTask (t : Task) : Task<unit> = t.ContinueWith ignore

    let processIdToStorageId (pid : string) = sprintf "process%s" <| pid.Substring(0,10)
    let defaultStorageId = "bootstrap"


    type Async with
        static member inline Cast<'U>(task : Async<obj>) = async { let! t = task in return box t :?> 'U }

    type AsyncBuilder with
        member inline __.Bind(f : Task<'T>, g : 'T -> Async<'S>) : Async<'S> = 
            async { let! r = Async.AwaitTask(f) in return! g r }

    type Uri with
        member u.ResourceId = u.Scheme
        
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
