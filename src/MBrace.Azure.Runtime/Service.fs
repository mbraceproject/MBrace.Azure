namespace Nessos.MBrace.Azure.Runtime

open System
open System.Threading
open System.Threading.Tasks
open Nessos.MBrace.Azure.Runtime
open System.Runtime.InteropServices
open Nessos.MBrace.Azure.Runtime.Common
open Nessos.MBrace.Runtime
open Nessos.MBrace

/// MBrace Runtime Service.
type Service (config : Configuration, maxTasks : int) =
    let id = guid ()
    do Configuration.Activate(config)
    let state = RuntimeState.FromConfiguration(config)

    member __.Configuration = config
    member __.Id = id
    member __.AttachLogger(logger) = state.ResourceFactory.Logger.Attach(logger)
    member __.MaxConcurrentTasks = maxTasks

    member __.StartAsTask() : Tasks.Task = Async.StartAsTask(__.AsyncStart()) :> _
        
    member __.AsyncStart() =
        async {
            let logf fmt = Printf.ksprintf state.ResourceFactory.Logger.Log fmt

            logf "Starting Service %s" id

            let! e = state.ResourceFactory.WorkerMonitor.DeclareCurrent(id)
            logf "Declared node %s : %d : %s" e.Hostname e.ProcessId (e :> IWorkerRef).Id

            Async.Start(state.ResourceFactory.WorkerMonitor.HeartbeatLoop())
            logf "Started heartbeat loop" 

            logf "Starting worker loop"
            return! Worker.initWorker state maxTasks
        }

    member __.Start() = Async.RunSynchronously(__.AsyncStart())
