namespace MBrace.Azure.Runtime

open MBrace.Runtime
open MBrace.Azure.Runtime.Utilities
open System
open MBrace.Core.Internals

(* Periodically performs the following tasks:
 * - Apply FaultException for jobs with affinity when worker is faulted.
 * - Set WorkerStatus to Stopped for workers that fail to give heartbeats.
 * - Set JobStatus for jobs that fail to give heartbeats.
 *)

[<Sealed; AutoSerializable(false)>]
type MaintenanceManager private (config : ConfigurationId, uuid : string, jobManager : JobManager, taskManager : TaskManager, workerManager : WorkerManager, logger : ISystemLogger) =

    let getSleepInterval (time : TimeSpan) : TimeSpan =
        let add = Random(hash uuid + int DateTime.UtcNow.Ticks).NextDouble() * 0.20 * time.TotalSeconds
                  |> TimeSpan.FromSeconds
        time + add

    let exec (task : Async<unit>) (sleepF : unit -> TimeSpan) (message : string) =
        let rec exec () = async {
            try
                logger.LogInfof "Starting maintenance task %A" message
                do! task
                logger.LogInfof "Maintenance %A complete" message
            with ex ->
                logger.LogWarningf "Maintenance %A failed with %A" message ex
            let sleep = sleepF()
            logger.LogInfof "Maintenance %A sleeping for %O" message sleep
            do! Async.Sleep(sleep)
            return! exec ()
        }

        Async.Start(async { 
                        do! Async.Sleep(sleepF())
                        do! exec()
                    })

    member this.Start () =
        exec (workerManager.FixWorkerState()) (fun () -> getSleepInterval WorkerManager.MaxHeartbeatTimespan) "Worker Status"

    static member Create(config, uuid, jobManager, taskManager, workerManager, logger) =
        new MaintenanceManager(config, uuid, jobManager, taskManager, workerManager, logger)