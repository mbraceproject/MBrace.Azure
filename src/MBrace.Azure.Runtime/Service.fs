namespace Nessos.MBrace.Azure.Runtime

open System
open System.Threading
open System.Threading.Tasks
open Nessos.MBrace.Azure.Runtime
open System.Runtime.InteropServices
open Nessos.MBrace.Azure.Runtime.Common
open Nessos.MBrace.Runtime

/// MBrace Runtime Service.
type Service (config : Configuration, state : RuntimeState, maxTasks : int) =
    // TODO : locks, checks
    let id = guid ()
    let mutable logger = NullLogger() :> ICloudLogger
    let logf fmt = Printf.ksprintf logger.Log fmt

    member __.Configuration = config
    member __.Id = id
    member __.Logger with get () = logger and set l = logger <- l
    member __.MaxConcurrentTasks = maxTasks

    member __.StartAsTask() : Tasks.Task = Async.StartAsTask(__.AsyncStart()) :> _
        
    member __.AsyncStart() =
        async {
            logf "Starting Service %s . . ." id
            logf "Initializing worker monitor . . ."
            let wmon = WorkerMonitor.Activate(Storage.defaultStorageId)

            let! e = wmon.DeclareCurrent(id)
            logf "Declared node %O/%s/%s . . ." e.CreationTime e.Id e.Hostname

            Async.Start(wmon.HeartbeatLoop(e))
            logf "Started heartbeat loop . . ." 

            logf "Starting worker loop . . ."
            return! Worker.initWorker state maxTasks logger
        }

    member __.Start() = Async.RunSynchronously(__.AsyncStart())
