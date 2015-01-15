module internal MBrace.Azure.Runtime.Worker

open System.Diagnostics
open System.Threading

open MBrace.Runtime
open MBrace.Continuation
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common
open MBrace.Azure.Runtime.Resources
open MBrace.Store

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
                            logf "Starting Root task for Process\n\tId:\"%s\"\n\tName:\"%s\"" task.ProcessInfo.Id task.ProcessInfo.Name
                            do! pmon.SetRunning(task.ProcessInfo.Id)

                        if msg.DeliveryCount = 1 then
                            do! pmon.AddActiveTask(task.ProcessInfo.Id)

                        logf "Starting task\n\t%s" (string task)
                        let sw = new Stopwatch()
                        sw.Start()
                        let! result = Async.Catch(runTask task dependencies (msg.DeliveryCount-1))
                        sw.Stop()

                        try
                            match result with
                            | Choice1Of2 () -> 
                                do! msg.CompleteAsync()
                                do! pmon.AddCompletedTask(task.ProcessInfo.Id)
                                logf "Completed task\n\t%s\n\tTime:%O" (string task) sw.Elapsed
                            | Choice2Of2 e -> 
                                do! msg.AbandonAsync()
                                do! pmon.AddFaultedTask(task.ProcessInfo.Id)
                                logf "Task fault %s with:\n%O" (string task) e
                        finally
                            wmon.DecrementTaskCount()
                        do! Async.Sleep 200
                    }
                    wmon.IncrementTaskCount()
                    let! _ = Async.StartChild(runTask())
                    ()
            with e -> 
                logf "WORKER FAULT: %A" e
                do! Async.Sleep 1000

            return! loop ()
    }

    return! loop ()
}
        