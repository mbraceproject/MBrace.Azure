namespace MBrace.Azure.Runtime

open System
open Microsoft.FSharp.Linq.NullableOperators
open Microsoft.WindowsAzure.Storage.Table
open MBrace.Azure
open MBrace.Azure.Runtime.Utilities
open MBrace.Runtime

[<AllowNullLiteral>]
type WorkerRecord(id) =
    inherit TableEntity(WorkerRecord.DefaultPartitionKey, id)
    
    member val Id                 = id with get, set
    member val Hostname           = Unchecked.defaultof<string> with get, set
    member val ProcessId          = Nullable<int>() with get, set
    member val ProcessName        = Unchecked.defaultof<string> with get, set
    member val InitializationTime = Nullable<DateTimeOffset>() with get, set
    member val ConfigurationId    = Unchecked.defaultof<byte []> with get, set
    member val MaxJobs            = Nullable<int>()   with get, set
    member val ActiveJobs         = Nullable<int>()   with get, set
    member val ProcessorCount     = Nullable<int>()   with get, set
    member val MaxClockSpeed      = Nullable<double>() with get, set
    member val CPU                = Nullable<double>() with get, set
    member val TotalMemory        = Nullable<double>() with get, set
    member val Memory             = Nullable<double>() with get, set
    member val NetworkUp          = Nullable<double>() with get, set
    member val NetworkDown        = Nullable<double>() with get, set
    member val Version            = Unchecked.defaultof<string> with get, set
    member val Status             = Unchecked.defaultof<byte []> with get, set
    
    new () = new WorkerRecord(null)

    member this.GetCounters () : Utils.PerformanceMonitor.PerformanceInfo =
        { 
            CpuUsage = this.CPU
            MaxClockSpeed = this.MaxClockSpeed 
            TotalMemory = this.TotalMemory
            MemoryUsage = this.Memory
            NetworkUsageUp = this.NetworkUp
            NetworkUsageDown = this.NetworkDown
        }

    member this.UpdateCounters(counters : Utils.PerformanceMonitor.PerformanceInfo) =
            this.CPU <- counters.CpuUsage
            this.TotalMemory <- counters.TotalMemory
            this.Memory <- counters.MemoryUsage
            this.NetworkUp <- counters.NetworkUsageUp
            this.NetworkDown <- counters.NetworkUsageDown
            this.MaxClockSpeed <- counters.MaxClockSpeed

    member this.CloneDefault() =
        let p = new WorkerRecord()
        p.PartitionKey <- this.PartitionKey
        p.RowKey <- this.RowKey
        p.ETag <- this.ETag
        p

    static member DefaultPartitionKey = "worker"

[<AutoSerializable(true)>]
type WorkerId internal (workerId) = 
    member this.Id = workerId

    interface IWorkerId with
        member this.CompareTo(obj: obj): int =
            match obj with
            | :? WorkerId as w -> compare workerId w.Id
            | _ -> invalidArg "obj" "invalid comparand."
        
        member this.Id: string = this.Id

    override this.ToString() = this.Id
    override this.Equals(other:obj) =
        match other with
        | :? WorkerId as w -> workerId = w.Id
        | _ -> false

    override this.GetHashCode() = hash workerId

[<AutoSerializable(true)>]
type WorkerManager private (config : ConfigurationId, logger : ISystemLogger) =

    let pickle (value : 'T) = Config.Pickler.Pickle(value)
    let unpickle (value : byte []) = Config.Pickler.UnPickle<'T>(value)

    let mkWorkerState (record : WorkerRecord) = 
        { Id = new WorkerId(record.Id)
          CurrentJobCount = record.ActiveJobs.GetValueOrDefault(-1)
          LastHeartbeat = record.Timestamp.DateTime
          HeartbeatRate = TimeSpan.MinValue // TODO : Implement
          InitializationTime = record.InitializationTime.Value.DateTime
          ExecutionStatus = unpickle record.Status
          PerformanceMetrics = record.GetCounters()
          Info = 
              { Hostname = record.Hostname
                ProcessId = record.ProcessId.GetValueOrDefault(-1)
                ProcessorCount = record.ProcessorCount.GetValueOrDefault(-1)
                MaxJobCount = record.MaxJobs.GetValueOrDefault(-1) } } 

    static member MaxHeartbeatTimespan = TimeSpan.FromMinutes(5.)

    member this.GetAllWorkers(): Async<WorkerState []> = 
        async { 
            let! records = Table.queryPK<WorkerRecord> config config.RuntimeTable WorkerRecord.DefaultPartitionKey
            let state = records |> Array.map mkWorkerState
            return state
        }

    member this.UnsubscribeWorker(id : IWorkerId) =
        async {
            logger.Logf LogLevel.Info "Unsubscribing worker %O" id
            let record = new WorkerRecord(id.Id)
            return! Table.delete config config.RuntimeTable record
        }

    interface IWorkerManager with
        member this.DeclareWorkerStatus(id: IWorkerId, status: WorkerJobExecutionStatus): Async<unit> = 
            async {
                let record = new WorkerRecord(id.Id)
                record.ETag <- "*"
                record.Status <- pickle status
                let! _ = Table.merge config config.RuntimeTable record
                return ()
            }
        
        member this.IncrementJobCount(id: IWorkerId): Async<unit> = 
            async {
                let! _ = Table.transact2<WorkerRecord> config config.RuntimeTable WorkerRecord.DefaultPartitionKey id.Id 
                            (fun e -> 
                                let ec = e.CloneDefault()
                                ec.ActiveJobs <- e.ActiveJobs ?+ 1
                                ec)
                return ()            
            }

        member this.DecrementJobCount(id: IWorkerId): Async<unit> = 
            async {
                let! _ = Table.transact2<WorkerRecord> config config.RuntimeTable WorkerRecord.DefaultPartitionKey id.Id 
                            (fun e -> 
                                let ec = e.CloneDefault()
                                ec.ActiveJobs <- e.ActiveJobs ?- 1
                                ec)
                return ()            
            }
        
        member this.GetAvailableWorkers(): Async<WorkerState []> = 
            async { 
                let! workers = this.GetAllWorkers()
                return workers 
                       |> Seq.filter (fun w -> DateTime.UtcNow - w.LastHeartbeat <= WorkerManager.MaxHeartbeatTimespan)
                       |> Seq.filter (fun w -> match w.ExecutionStatus with
                                               | WorkerJobExecutionStatus.Running -> true
                                               | _ -> false)
                       |> Seq.toArray
            }
        
        member this.SubmitPerformanceMetrics(id: IWorkerId, perf: Utils.PerformanceMonitor.PerformanceInfo): Async<unit> = 
            async {
                let record = new WorkerRecord(id.Id)
                record.ETag <- "*"
                record.UpdateCounters(perf)
                let! _result = Table.merge config config.RuntimeTable record
                return ()
            }
        
        member this.SubscribeWorker(id: IWorkerId, info: WorkerInfo): Async<IDisposable> = 
            async {
                logger.Logf LogLevel.Info "Subscribing worker %O" id
                let joined = DateTimeOffset.UtcNow
                let record = new WorkerRecord(id.Id)
                record.Hostname <- info.Hostname
                record.ProcessName <- Diagnostics.Process.GetCurrentProcess().ProcessName
                record.ProcessId <- nullable info.ProcessId
                record.InitializationTime <- nullable joined
                record.ActiveJobs <- nullable 0
                record.Status <- pickle WorkerJobExecutionStatus.Running
                record.Version <- ReleaseInfo.localVersion.ToString(4)
                record.MaxJobs <- nullable info.MaxJobCount
                record.ProcessorCount <- nullable info.ProcessorCount
                record.ConfigurationId <- pickle config
                do! Table.insertOrReplace<WorkerRecord> config config.RuntimeTable record //Worker might restart but keep id.
                let unsubscriber =
                    { new IDisposable with
                          member x.Dispose(): unit = 
                            this.UnsubscribeWorker(id)
                            |> Async.RunSynchronously
                    }
                return unsubscriber
            }
        
        member this.TryGetWorkerState(id: IWorkerId): Async<WorkerState option> = 
            async {
                let! record = Table.read<WorkerRecord> config config.RuntimeTable WorkerRecord.DefaultPartitionKey id.Id
                if record = null then return None
                else return Some(mkWorkerState record)
            }
        

    static member Create(config : ConfigurationId, logger) =
        new WorkerManager(config, logger)
