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
               (maxConcurrentTasks : int)
               (resources : ResourceRegistry)
               (store : ICloudFileStore)
               (channel : ICloudChannelProvider)
               (atom : ICloudAtomProvider) : Async<unit> = async {

    let pmon = resources.Resolve<ProcessMonitor>()
    let wmon = resources.Resolve<WorkerMonitor>()
    let logger = resources.Resolve<ILogger>()

    let runTask task deps faultCount  =
        let provider = RuntimeProvider.FromTask runtime wmon deps task
        let info = task.ProcessInfo
        let resources = resource { 
            yield! resources
            yield { FileStore = defaultArg info.FileStore store ; DefaultDirectory = info.DefaultDirectory }
            yield { AtomProvider = defaultArg info.AtomProvider atom ; DefaultContainer = info.DefaultAtomContainer }
            yield { ChannelProvider = defaultArg info.ChannelProvider channel; DefaultContainer = info.DefaultChannelContainer }
        }
        Task.RunAsync provider resources deps faultCount task
    let inline logf fmt = Printf.ksprintf logger.Log fmt

    let rec loop () = async {
        if wmon.ActiveTasks >= maxConcurrentTasks then
            do! Async.Sleep 100
            return! loop ()
        else
            try
                let! task = runtime.TryDequeue()
                match task with
                | None -> do! Async.Sleep 100
                | Some (msg, task, dependencies) ->
                    let runTask () = async {
                        let! _ = Async.StartChild(msg.RenewLoopAsync())

                        if task.TaskType = TaskType.Root then
                            logf "Running root task for process %s %s" task.ProcessInfo.Id task.ProcessInfo.Name
                            do! pmon.SetRunning(task.ProcessInfo.Id)

                        if msg.DeliveryCount = 1 then
                            do! pmon.AddActiveTask(task.ProcessInfo.Id)
                        wmon.IncrementTaskCount()

                        logf "Starting task %s" (string task)
                        let sw = new Stopwatch()
                        sw.Start()
                        let! result = Async.Catch(runTask task dependencies (msg.DeliveryCount-1))
                        sw.Stop()

                        match result with
                        | Choice1Of2 () -> 
                            do! msg.CompleteAsync()
                            do! pmon.AddCompletedTask(task.ProcessInfo.Id)
                            logf "Completed task %s in %O" (string task) sw.Elapsed
                        | Choice2Of2 e -> 
                            do! msg.AbandonAsync()
                            do! pmon.AddFaultedTask(task.ProcessInfo.Id)
                            logf "Task fault %s with:\n%O" (string task) e
                        wmon.DecrementTaskCount()
                        do! Async.Sleep 200
                    }
        
                    let! _ = Async.StartChild(runTask())
                    ()
            with e -> 
                logf "WORKER FAULT: %A" e
                do! Async.Sleep 1000

            return! loop ()
    }

    return! loop ()
}
        