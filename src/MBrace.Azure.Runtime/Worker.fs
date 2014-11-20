module internal Nessos.MBrace.Azure.Runtime.Worker

open System.Diagnostics
open System.Threading

open Nessos.MBrace.Runtime
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Common
open Nessos.MBrace.Azure.Runtime.Resources

/// <summary>
///     Initializes a worker loop. Worker polls task queue of supplied
///     runtime for available tasks and executes as appropriate.
/// </summary>
/// <param name="runtime">Runtime to subscribe to.</param>
/// <param name="maxConcurrentTasks">Maximum tasks to be executed concurrently by worker.</param>
/// <param name="logf">Logger.</param>
let initWorker (runtime : RuntimeState) 
               (maxConcurrentTasks : int)
               (logger : ICloudLogger) : Async<unit> = async {

    let currentTaskCount = ref 0
    let runTask procId deps t =
        let provider = RuntimeProvider.FromTask runtime procId deps t
        Task.RunAsync provider deps t
    let inline logf fmt = Printf.ksprintf logger.Log fmt

    let rec loop () = async {
        if !currentTaskCount >= maxConcurrentTasks then
            do! Async.Sleep 50
            return! loop ()
        else
            try
                let! tasks = runtime.DequeueBatch(maxConcurrentTasks - !currentTaskCount)
                if Array.isEmpty tasks then
                    do! Async.Sleep 50
                else
                    for (task, procId, dependencies) in tasks do
                        let _ = Interlocked.Increment currentTaskCount
                        let runTask () = async {
                            logf "Starting task %s/%s/%O." procId task.TaskId task.Type

                            let sw = new Stopwatch()
                            sw.Start()
                            let! result = runTask procId dependencies task |> Async.Catch
                            sw.Stop()

                            match result with
                            | Choice1Of2 () -> 
                                logf "Completed task %s/%s/%O." procId task.TaskId sw.Elapsed
                            | Choice2Of2 e -> 
                                logf "Task fault %s/%s with: \n%O." procId task.TaskId e

                            let _ = Interlocked.Decrement currentTaskCount
                            return ()
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
        