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
            let id = guid()
            let logger = StorageLogger.Activate(Storage.defaultLogId, "worker", id)
            logger.Attach logf.Invoke
            do! logger.AsyncLogf "Logger initialized..."
            do! logger.AsyncLogf "Initializing worker monitor..."
            let wmon = Common.WorkerMonitor.Activate(Storage.defaultStorageId)
            let! e = wmon.DeclareCurrent(id)
            do! logger.AsyncLogf "Declared node %s %s..." e.Id e.Hostname
            Async.Start(wmon.HeartbeatLoop(e))
            do! logger.AsyncLogf "Started heartbeat loop..." 
            do! logger.AsyncLogf "Starting worker loop..."
            return! Worker.initWorker state maxConcurrentTasks logger
        }
