namespace MBrace.Azure.Runtime

open MBrace.Runtime
open MBrace.Azure.Runtime.Utilities
open System
open MBrace.Core.Internals

(* Periodically performs the following tasks
 * - Apply FaultException for jobs with affinity when worker is faulted.
 * - Set WorkerStatus to Stopped for workers that fail to give heartbeats.
 * - Set JobStatus for jobs that fail to give heartbeats.
 *)

[<Sealed; AutoSerializable(false)>]
type MaintenanceManager private (config : ConfigurationId, uuid : string, jobManager : JobManager, taskManager : TaskManager, workerManager : WorkerManager, logger : ISystemLogger) =

    let getSleepInterval (time : TimeSpan, dev : TimeSpan) : TimeSpan =
        let add = Random(hash uuid).NextDouble() * dev.TotalSeconds
                  |> TimeSpan.FromSeconds
        time + add

    let checkWorkerStatus () = async {
        let sleep () = getSleepInterval(WorkerManager.MaxHeartbeatTimespan, TimeSpan.FromMilliseconds(WorkerManager.MaxHeartbeatTimespan.TotalMilliseconds / 5.) )
        
        let rec checkWorkerStatusAux () = async {
            try
                logger.LogInfo "Starting maintenance task : checking worker status"
                let! workers = workerManager.GetAllWorkers()
                let now = DateTime.UtcNow
                let nonResponsiveWorkers = 
                    workers 
                    |> Seq.filter (fun w -> 
                        match w.ExecutionStatus with
                        | WorkerJobExecutionStatus.Running when now - w.LastHeartbeat > WorkerManager.MaxHeartbeatTimespan -> true
                        | _ -> false)
        
                logger.LogInfof "Maintenance : found %d non-responsive workers, changing status" (Seq.length nonResponsiveWorkers)
        
                // TODO : Should we set status to Stopped, or add an extra faulted status?
                // Using QueueFault for now.
                let mkFault worker = 
                    QueueFault <| ExceptionDispatchInfo.Capture(RuntimeException(sprintf "Worker %O failed to give heartbeat." worker))

                do! nonResponsiveWorkers
                    |> Seq.map (fun w -> async {
                        try 
                            do! (workerManager :> IWorkerManager).DeclareWorkerStatus(w.Id, mkFault w.Id) 
                        with
                        | ex -> 
                            logger.LogWarningf "Maintenance : failed to change status for worker %O : %A" w.Id ex
                            return ()
                        })
                    |> Async.Parallel
                    |> Async.Ignore
            with ex ->
                logger.LogWarningf "Maintenance : failed with %A" ex

            let sleepTime = sleep()
            logger.LogInfof "Maintenance : worker status maintenance complete, sleep for %O" sleepTime
            do! Async.Sleep(sleepTime)
            return! checkWorkerStatusAux ()
        }

        do! Async.Sleep(sleep())
        return! checkWorkerStatusAux()
    }

    member this.Start () =
        logger.LogInfo "Maintenance : starting worker maintenance loop"
        Async.Start(checkWorkerStatus())

    static member Create(config, uuid, jobManager, taskManager, workerManager, logger) =
        new MaintenanceManager(config, uuid, jobManager, taskManager, workerManager, logger)