namespace Nessos.MBrace.Azure.Runtime

open System
open System.Threading
open System.Threading.Tasks
open Nessos.MBrace.Azure.Runtime
open System.Runtime.InteropServices
open Nessos.MBrace.Azure.Runtime.Common

[<Sealed; AbstractClass>]
type Service private () =

    // TODO : locks, checks
    static member Configuration with set cfg = Configuration.Initialize(cfg)

    static member StartAsTask(state : RuntimeState, logf : Action<string>, maxConcurrentTasks : int) : Tasks.Task =
        Async.StartAsTask(Service.AsyncStart(state, logf, maxConcurrentTasks)) :> _
        
    static member AsyncStart(state : RuntimeState, logf : Action<string>, maxConcurrentTasks : int) =
        async {
            let inline logfn fmt = Printf.ksprintf logf.Invoke fmt
            logfn "Initializing worker monitor..."
            let wmon = Common.WorkerMonitor.Activate(Storage.defaultStorageId)
            let! e = wmon.DeclareCurrent()
            logfn "Declared node %s %s..." e.Id e.Hostname
            Async.Start(wmon.HeartbeatLoop(e))
            logfn "Started heartbeat loop..." 
            logfn "Starting worker loop..."
            return! Worker.initWorker state maxConcurrentTasks logf.Invoke
        }
