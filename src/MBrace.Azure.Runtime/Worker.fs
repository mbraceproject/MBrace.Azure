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
let initWorker (runtime : RuntimeState) 
               (maxConcurrentTasks : int) : Async<unit> = async {

    let currentTaskCount = ref 0
    let runTask procId deps t =
        let provider = RuntimeProvider.FromTask runtime procId deps t
        Task.RunAsync provider deps t
    let inline logf fmt = Printf.ksprintf runtime.ResourceFactory.Logger.Log fmt

    let rec loop () = async {
        if !currentTaskCount >= maxConcurrentTasks then
            do! Async.Sleep 50
            return! loop ()
        else
            try
                let! task = runtime.TryDequeue()
                match task with
                | None ->
                    do! Async.Sleep 50
                | Some (msg, task, procId, dependencies) ->
                    let _ = Interlocked.Increment currentTaskCount
                    let runTask () = async {
                        logf "Starting task %s/%s/%d/%O" procId task.TaskId msg.DeliveryCount task.Type

                        let! renew = Async.StartChild(msg.RenewLoopAsync())
                        logf "Started task renew loop"

                        let sw = new Stopwatch()
                        sw.Start()
                        let! result = runTask procId dependencies task |> Async.Catch
                        sw.Stop()

                        match result with
                        | Choice1Of2 () -> 
                            do! msg.CompleteAsync()
                            logf "Completed task %s/%s/%O" procId task.TaskId sw.Elapsed
                        | Choice2Of2 e -> 
                            do! msg.AbandonAsync()
                            logf "Task fault %s/%s with: \n%O" procId task.TaskId e

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
        