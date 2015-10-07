namespace MBrace.Azure.Runtime

open System

open Microsoft.FSharp.Linq.NullableOperators
open Microsoft.WindowsAzure.Storage.Table

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Azure
open MBrace.Azure.Runtime.Utilities
open MBrace.Runtime

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

    /// Gets all worker records.
    member this.GetAllWorkers(): Async<WorkerState []> = async { 
        let! records = Table.queryPK<WorkerRecord> clusterId.StorageAccount clusterId.RuntimeTable WorkerRecord.DefaultPartitionKey
        let state = records |> Seq.map mkWorkerState |> Seq.toArray
        return state
    }

    /// 'Running' workers that fail to give heartbeats.
    member this.GetNonResponsiveWorkers (?heartbeatThreshold : TimeSpan) : Async<WorkerState []> = async {
        let now = DateTimeOffset.Now
        let! workers = this.GetAllWorkers()
        return workers |> Array.filter (fun w -> 
                            match w.ExecutionStatus with
                            | CloudWorkItemExecutionStatus.Running when now - w.LastHeartbeat > defaultArg heartbeatThreshold w.Info.HeartbeatThreshold -> true
                            | _ -> false)
    }

    /// Workers that fail to give heartbeats.
    member this.GetInactiveWorkers () : Async<WorkerState []> = async {
        let now = DateTimeOffset.Now
        let! workers = this.GetAllWorkers()
        return workers |> Array.filter (fun w -> now - w.LastHeartbeat > w.Info.HeartbeatThreshold)
    }

    /// Get workers that are active and actively sending heartbeats
    member this.GetAvailableWorkers(): Async<WorkerState []> = async { 
        let now = DateTimeOffset.Now
        let! workers = this.GetAllWorkers()
        return 
            workers 
            |> Array.filter (fun w -> 
                match w.ExecutionStatus with
                | CloudWorkItemExecutionStatus.Running when now - w.LastHeartbeat <= w.Info.HeartbeatThreshold -> true
                | _ -> false)
    }

    /// Culls workers that have stopped sending heartbeats in a timespan larger than specified threshold
    member this.CullNonResponsiveWorkers(heartbeatThreshold : TimeSpan) = async {
        if heartbeatThreshold < TimeSpan.FromSeconds 5. then invalidArg "heartbeatThreshold" "Must be at least 5 seconds."
        let! nonResponsiveWorkers = this.GetNonResponsiveWorkers(heartbeatThreshold)
        let level = if nonResponsiveWorkers.Length > 0 then LogLevel.Warning else LogLevel.Info
        logger.Logf level "WorkerManager : found %d non-responsive workers" nonResponsiveWorkers.Length
        let cullWorker(worker : WorkerState) = async {
            try 
                logger.Logf LogLevel.Info "WorkerManager : culling inactive worker '%O'" worker.Id.Id
                let e = new FaultException(sprintf "Worker '%O' failed to give heartbeat." worker.Id)
                // TODO : change QueueFault to WorkerDeath
                do! (this :> IWorkerManager).DeclareWorkerStatus(worker.Id, QueueFault <| ExceptionDispatchInfo.Capture e)
            with e ->
                logger.Logf LogLevel.Warning "WorkerManager : failed to change status for worker '%O':\n%O" worker.Id e
        }

        do! nonResponsiveWorkers |> Seq.map cullWorker |> Async.Parallel |> Async.Ignore
    }

    member this.UnsubscribeWorker(workerId : IWorkerId) = async {
        logger.Logf LogLevel.Info "Unsubscribing worker %O" workerId
        return! (this :> IWorkerManager).DeclareWorkerStatus(workerId, CloudWorkItemExecutionStatus.Stopped)
    }

    interface IWorkerManager with
        member this.DeclareWorkerStatus(workerId: IWorkerId, status: CloudWorkItemExecutionStatus): Async<unit> = async {
            logger.LogInfof "Changing worker %O status to %A" workerId status
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
        
        member this.GetAvailableWorkers(): Async<WorkerState []> = this.GetAvailableWorkers()
        
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
