namespace MBrace.Azure.Runtime

open System
open Microsoft.FSharp.Linq.NullableOperators
open Microsoft.WindowsAzure.Storage.Table
open MBrace.Azure
open MBrace.Azure.Runtime.Utilities
open MBrace.Runtime
open MBrace.Core.Internals

[<AutoSerializable(false)>]
type WorkerManager private (clusterId : ClusterId, logger : ISystemLogger) =

    let pickle (value : 'T) = ProcessConfiguration.Serializer.Pickle(value)
    let unpickle (value : byte []) = ProcessConfiguration.Serializer.UnPickle<'T>(value)

    let mkWorkerState (record : WorkerRecord) =
        let workerInfo =
            { 
                Hostname = record.Hostname
                ProcessId = record.ProcessId.GetValueOrDefault(-1)
                ProcessorCount = record.ProcessorCount.GetValueOrDefault(-1)
                MaxWorkItemCount = record.MaxWorkItems.GetValueOrDefault(-1) 
                HeartbeatInterval = record.HeartbeatInterval.Value |> TimeSpan.FromTicks
                HeartbeatThreshold = record.HeartbeatThreshold.Value |> TimeSpan.FromTicks
            }
            
        { 
            Id = new WorkerId(record.Id)
            CurrentWorkItemCount = record.ActiveWorkItems.GetValueOrDefault(-1)
            LastHeartbeat = record.LastHeartbeat.Value
            InitializationTime = record.InitializationTime.Value
            ExecutionStatus = unpickle record.Status
            PerformanceMetrics = record.GetCounters()
            Info = workerInfo
        } 

//    /// Attempts to find non-responsive workers and fix their status. Returns whether any non-responsive workers were found.
//    let rec cleanup () : Async<unit> =
//        async { 
//            do! Async.Sleep(int(0.2 * WorkerManager.MaxHeartbeatTimespan.TotalMilliseconds))
//            let! result = Async.Catch <| async {
//                logger.LogInfo "WorkerManager : checking worker status"
//
//                let! nonResponsiveWorkers = this.GetNonResponsiveWorkers()
//
//                let level = if nonResponsiveWorkers.Length > 0 then LogLevel.Warning else LogLevel.Info
//                logger.Logf level "WorkerManager : found %d non-responsive workers" nonResponsiveWorkers.Length
//                // TODO : Should we set status to Stopped, or add an extra faulted status?
//                // Using QueueFault for now.
//                let mkFault worker = 
//                    let e = RuntimeException(sprintf "Worker %O failed to give heartbeat." worker)
//                    let edi = ExceptionDispatchInfo.Capture e
//                    QueueFault edi
//
//                do! nonResponsiveWorkers
//                    |> Array.map (fun w -> 
//                        async { 
//                            try 
//                                do! (this :> IWorkerManager).DeclareWorkerStatus(w.Id, mkFault w.Id)
//                            with ex -> 
//                                logger.LogWarningf "WorkerManager : failed to change status for worker %O : %A" w.Id ex
//                                return ()
//                        })
//                    |> Async.Parallel
//                    |> Async.Ignore
//            }
//
//            match result with
//            | Choice1Of2 () -> logger.LogInfo "WorkerManager : maintenance complete."
//            | Choice2Of2 ex -> logger.LogWarningf "WorkerManager : maintenance failed with : %A" ex
//
//            return! cleanup ()
//        }

    ///// Start worker maintenance service.
    //member this.EnableMaintenance () = Async.Start(cleanup())

    /// 'Running' workers that fail to give heartbeats.
    member this.GetNonResponsiveWorkers () : Async<WorkerState []> = async {
        let! workers = this.GetAllWorkers()
        let now = DateTimeOffset.Now
        return workers |> Array.filter (fun w -> 
                            match w.ExecutionStatus with
                            | CloudWorkItemExecutionStatus.Running when now - w.LastHeartbeat > w.Info.HeartbeatThreshold -> true
                            | _ -> false)
    }

    /// Workers that fail to give heartbeats.
    member this.GetInactiveWorkers () : Async<WorkerState []> = async {
        let! workers = this.GetAllWorkers()
        let now = DateTimeOffset.Now
        return workers |> Array.filter (fun w -> now - w.LastHeartbeat > w.Info.HeartbeatThreshold)
    }

    member this.GetAllWorkers(): Async<WorkerState []> = async { 
        let! records = Table.queryPK<WorkerRecord> clusterId.StorageAccount clusterId.RuntimeTable WorkerRecord.DefaultPartitionKey
        let state = records |> Seq.map mkWorkerState |> Seq.toArray
        return state
    }

    member this.UnsubscribeWorker(workerId : IWorkerId) = async {
        logger.Logf LogLevel.Info "Unsubscribing worker %O" workerId
        return! (this :> IWorkerManager).DeclareWorkerStatus(workerId, CloudWorkItemExecutionStatus.Stopped)
    }

    interface IWorkerManager with
        member this.DeclareWorkerStatus(workerId: IWorkerId, status: CloudWorkItemExecutionStatus): Async<unit> = async {
            logger.LogInfof "Changing worker %O status to %A" clusterId status
            let record = new WorkerRecord(workerId.Id)
            record.ETag <- "*"
            record.Status <- pickle status
            let! _ = Table.merge clusterId.StorageAccount clusterId.RuntimeTable record
            return ()
        }
        
        member this.IncrementWorkItemCount(workerId: IWorkerId): Async<unit> = async {
            let! _ = Table.transact2<WorkerRecord> clusterId.StorageAccount clusterId.RuntimeTable WorkerRecord.DefaultPartitionKey workerId.Id 
                        (fun e -> 
                            let ec = e.CloneDefault()
                            ec.ActiveWorkItems <- e.ActiveWorkItems ?+ 1
                            ec)
            return ()            
        }

        member this.DecrementWorkItemCount(workerId: IWorkerId): Async<unit> = async {
            let! _ = Table.transact2<WorkerRecord> clusterId.StorageAccount clusterId.RuntimeTable WorkerRecord.DefaultPartitionKey workerId.Id 
                        (fun e -> 
                            let ec = e.CloneDefault()
                            ec.ActiveWorkItems <- e.ActiveWorkItems ?- 1
                            ec)
            return ()            
        }
        
        member this.GetAvailableWorkers(): Async<WorkerState []> = async { 
            let! workers = this.GetAllWorkers()
            return 
                workers 
                |> Seq.filter (fun w -> DateTimeOffset.Now - w.LastHeartbeat <= w.Info.HeartbeatThreshold)
                |> Seq.filter (fun w -> match w.ExecutionStatus with
                                        | CloudWorkItemExecutionStatus.Running -> true
                                        | _ -> false)
                |> Seq.toArray
        }
        
        member this.SubmitPerformanceMetrics(workerId: IWorkerId, perf: Utils.PerformanceMonitor.PerformanceInfo): Async<unit> = async {
            let record = new WorkerRecord(workerId.Id)
            record.ETag <- "*"
            record.UpdateCounters(perf)
            let! _result = Table.merge clusterId.StorageAccount clusterId.RuntimeTable record
            return ()
        }
        
        member this.SubscribeWorker(workerId: IWorkerId, info: WorkerInfo): Async<IDisposable> = async {
            logger.Logf LogLevel.Info "Subscribing worker %O" clusterId
            let joined = DateTimeOffset.UtcNow
            let record = new WorkerRecord(workerId.Id)
            record.Hostname <- info.Hostname
            record.ProcessName <- Diagnostics.Process.GetCurrentProcess().ProcessName
            record.ProcessId <- nullable info.ProcessId
            record.InitializationTime <- nullable joined
            record.LastHeartbeat <- nullable joined
            record.ActiveWorkItems <- nullable 0
            record.Status <- pickle CloudWorkItemExecutionStatus.Running
            record.Version <- ProcessConfiguration.Version.ToString(4)
            record.MaxWorkItems <- nullable info.MaxWorkItemCount
            record.ProcessorCount <- nullable info.ProcessorCount
            record.ConfigurationId <- pickle clusterId
            record.HeartbeatInterval <- nullable info.HeartbeatInterval.Ticks
            record.HeartbeatThreshold <- nullable info.HeartbeatThreshold.Ticks
            do! Table.insertOrReplace<WorkerRecord> clusterId.StorageAccount clusterId.RuntimeTable record //Worker might restart but keep id.
            let unsubscriber =
                { 
                    new IDisposable with
                        member x.Dispose(): unit = 
                            this.UnsubscribeWorker(workerId)
                            |> Async.RunSync
                }
            return unsubscriber
        }
        
        member this.TryGetWorkerState(workerId: IWorkerId): Async<WorkerState option> = async {
            let! record = Table.read<WorkerRecord> clusterId.StorageAccount clusterId.RuntimeTable WorkerRecord.DefaultPartitionKey workerId.Id
            if record = null then return None
            else return Some(mkWorkerState record)
        }

    static member Create(clusterId : ClusterId, logger : ISystemLogger) =
        new WorkerManager(clusterId, logger)
