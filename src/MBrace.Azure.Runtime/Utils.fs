[<AutoOpen>]
module Nessos.MBrace.Azure.Runtime.Utils

    open System
    open System.Threading.Tasks
    open Microsoft.WindowsAzure.Storage.Table

    let guid() = Guid.NewGuid().ToString("N")
    let uri fmt = Printf.ksprintf (fun s -> new Uri(s)) fmt
    
    let toContainerId (res : Uri) = 
        let s = res.Segments.[0]
        s.Substring(0, s.Length - 1), res.Segments.[1]

    let inline ofTask (t : Task) : Task<unit> = t.ContinueWith ignore

    type AsyncBuilder with
        member inline __.Bind(f : Task<'T>, g : 'T -> Async<'S>) : Async<'S> = 
            async { let! r = Async.AwaitTask(f) in return! g r }
