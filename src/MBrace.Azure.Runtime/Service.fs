namespace Nessos.MBrace.Azure.Runtime

open System
open System.Threading
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Tasks
open System.Runtime.InteropServices

type Service private () =

    // TODO : change
    static let mutable cts : CancellationTokenSource = null
    static member Configuration with set cfg = Configuration.Initialize(cfg)

    static member Start(state : RuntimeState, maxConcurrentTasks : int) =
        cts <- new CancellationTokenSource()
        Async.Start(Worker.initWorker state maxConcurrentTasks, cts.Token)

    // TODO : Change
    static member StartSync(state : RuntimeState, maxConcurrentTasks : int) =
        Async.RunSynchronously(Worker.initWorker state maxConcurrentTasks)

    static member Stop () = cts.Cancel()
