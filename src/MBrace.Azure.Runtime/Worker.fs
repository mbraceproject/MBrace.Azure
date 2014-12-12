module internal Nessos.MBrace.Azure.Runtime.Worker

open System.Diagnostics
open System.Threading

open Nessos.MBrace.Runtime
open Nessos.MBrace.Continuation
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Common
open Nessos.MBrace.Azure.Runtime.Resources
open Nessos.MBrace.Store

/// <summary>
///     Initializes a worker loop. Worker polls task queue of supplied
///     runtime for available tasks and executes as appropriate.
/// </summary>
/// <param name="runtime">Runtime to subscribe to.</param>
/// <param name="maxConcurrentTasks">Maximum tasks to be executed concurrently by worker.</param>
let initWorker (runtime : RuntimeState) 
               (resources : ResourceRegistry)
               (maxConcurrentTasks : int) : Async<unit> = async {

    let pmon = resources.Resolve<ProcessMonitor>()
    let wmon = resources.Resolve<WorkerMonitor>()
    let logger = resources.Resolve<ILogger>()

    let currentTaskCount = ref 0
    let runTask procId deps faultCount t =
        let provider = RuntimeProvider.FromTask runtime wmon procId deps t
        Task.RunAsync provider resources deps faultCount t
    let inline logf fmt = Printf.ksprintf logger.Log fmt

    let rec loop () = async {
        if !currentTaskCount >= maxConcurrentTasks then
            do! Async.Sleep 500
            return! loop ()
        else
            try
                let! task = runtime.TryDequeue()
                match task with
                | None -> do! Async.Sleep 500
                | Some (msg, task, procId, dependencies) ->
                    let count = Interlocked.Increment currentTaskCount
                    do! wmon.SetCurrentTaskCount(count)
                    let runTask () = async {
                        if task.TaskType = TaskType.Root then
                            logf "Running root task for process %s" task.ProcessId
                            do! pmon.SetRunning(task.ProcessId)

                        logf "Starting task %s" (string task)
                        do! wmon.IncreaseTotalTaskCount()
                        let! renew = Async.StartChild(msg.RenewLoopAsync())

                        let sw = new Stopwatch()
                        sw.Start()
                        let! result = Async.Catch(runTask procId dependencies (msg.DeliveryCount-1) task)
                        sw.Stop()

                        match result with
                        | Choice1Of2 () -> 
                            do! msg.CompleteAsync()
                            logf "Completed task %s in %O" (string task) sw.Elapsed
                        | Choice2Of2 e -> 
                            do! msg.AbandonAsync()
                            logf "Task fault %s with:\n%O" (string task) e
                        do! wmon.IncreaseCompletedTaskCount()
                        let count = Interlocked.Decrement currentTaskCount
                        do! wmon.SetCurrentTaskCount(count)
                        do! Async.Sleep 200
                    }
        
                    let! handle = Async.StartChild(runTask())
                    ()
            with e -> 
                logf "WORKER FAULT: %O" e
                do! Async.Sleep 1000

            return! loop ()
    }

    return! loop ()
}
        