namespace Nessos.MBrace.Azure.Runtime

open System
open System.Threading.Tasks
open Nessos.MBrace.Azure.Runtime
open System.Runtime.InteropServices

[<Sealed; AbstractClass>]
type Service private () =

    // TODO : locks, checks
    static member Configuration with set cfg = Configuration.Initialize(cfg)

    static member StartAsTask(state : Tasks.RuntimeState, logf : Action<string>, maxConcurrentTasks : int) : Task =
        Async.StartAsTask(Service.AsyncStart(state, logf, maxConcurrentTasks)) :> _
        
    static member AsyncStart(state : Tasks.RuntimeState, logf : Action<string>, maxConcurrentTasks : int) =
        Worker.initWorker state maxConcurrentTasks logf.Invoke
