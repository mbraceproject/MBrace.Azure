namespace MBrace.Azure.Runtime

open System
open Microsoft.FSharp.Linq.NullableOperators
open Microsoft.WindowsAzure.Storage.Table
open MBrace.Azure
open MBrace.Azure.Runtime.Utilities
open MBrace.Runtime

type WorkerRecord(pk, id, hostname : string, pid : Nullable<int>, pname : string, joined : DateTimeOffset) =
    inherit TableEntity(pk, id)
    
    member val Hostname           = hostname          with get, set
    member val Id                 = id                with get, set
    member val ProcessId          = pid               with get, set
    member val ProcessName        = pname             with get, set
    member val InitializationTime = joined            with get, set
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
    
    new () = new WorkerRecord(null, null, null, nullableDefault, null, Unchecked.defaultof<_>)
    new (pk, id) = new WorkerRecord(pk, id, null, nullableDefault, null, Unchecked.defaultof<_>)

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

type internal HeartbeatMonitor(config : ConfigurationId, id : WorkerId) =
    interface IDisposable with
        member this.Dispose(): unit = 
            failwith "Not implemented yet"

[<AutoSerializable(true)>]
type WorkerManager private (config : ConfigurationId) =
    let partitionKey = "WorkerRef"
    let table = config.RuntimeTable
    let maxHeartbeatTimespan = TimeSpan.FromMinutes(10.)

    let pickle (value : 'T) = Configuration.Pickler.Pickle(value)
    let unpickle (value : byte []) = Configuration.Pickler.UnPickle<'T>(value)

    let mkWorkerState (record : WorkerRecord) = 
        { Id = new WorkerId(record.Id)
          CurrentJobCount = record.ActiveJobs.GetValueOrDefault(-1)
          LastHeartbeat = record.Timestamp.DateTime
          HeartbeatRate = TimeSpan.FromDays(1.) // TODO
          InitializationTime = record.InitializationTime.DateTime
          ExecutionStatus = unpickle record.Status
          PerformanceMetrics = record.GetCounters()
          Info = 
              { Hostname = record.Hostname
                ProcessId = record.ProcessId.GetValueOrDefault(-1)
                ProcessorCount = record.ProcessorCount.GetValueOrDefault(-1)
                MaxJobCount = record.MaxJobs.GetValueOrDefault(-1) } } 

    member this.GetAllWorkers(): Async<WorkerState []> = 
        async { 
            let! records = Table.queryPK<WorkerRecord> config table partitionKey
            let state = records |> Array.map mkWorkerState
            return state
        }

    interface IWorkerManager with
        member this.DeclareWorkerStatus(id: IWorkerId, status: WorkerJobExecutionStatus): Async<unit> = 
            async {
                let record = new WorkerRecord(partitionKey, id.Id)
                record.Status <- pickle status
                let! _ = Table.merge config table record
                return ()
            }
        
        member this.IncrementJobCount(id: IWorkerId): Async<unit> = 
            async {
                let! _ = Table.transact<WorkerRecord> config table partitionKey id.Id 
                            (fun e -> e.ActiveJobs <- e.ActiveJobs ?+ 1)
                return ()            
            }

        member this.DecrementJobCount(id: IWorkerId): Async<unit> = 
            async {
                let! _ = Table.transact<WorkerRecord> config table partitionKey id.Id 
                            (fun e -> e.ActiveJobs <- e.ActiveJobs ?- 1)
                return ()            
            }
        
        member this.GetAvailableWorkers(): Async<WorkerState []> = 
            async { 
                let! workers = this.GetAllWorkers()
                return workers |> Array.filter (fun w -> DateTime.UtcNow - w.LastHeartbeat <= maxHeartbeatTimespan)
            }
        
        member this.SubmitPerformanceMetrics(id: IWorkerId, perf: Utils.PerformanceMonitor.PerformanceInfo): Async<unit> = 
            async {
                let record = new WorkerRecord(partitionKey, id.Id)
                record.UpdateCounters(perf)
                let! _ = Table.merge config table record
                return ()
            }
        
        member this.SubscribeWorker(id: IWorkerId, info: WorkerInfo): Async<IDisposable> = 
            async {
                let ps = Diagnostics.Process.GetCurrentProcess()
                let joined = DateTimeOffset.UtcNow
                let record = new WorkerRecord(partitionKey, id.Id, info.Hostname, nullable ps.Id, ps.ProcessName, joined)
                record.ActiveJobs <- nullable 0
                record.Status <- pickle WorkerJobExecutionStatus.Running
                record.Version <- ReleaseInfo.localVersion.ToString(4)
                record.MaxJobs <- nullable info.MaxJobCount
                record.ProcessorCount <- nullable info.ProcessorCount
                record.ConfigurationId <- pickle config
                do! Table.insertOrReplace<WorkerRecord> config table record //Worker might restart but keep id.
                return null :> IDisposable
            }
        
        member this.TryGetWorkerState(id: IWorkerId): Async<WorkerState option> = 
            failwith "Not implemented yet"
        

    static member Create(config : ConfigurationId) =
        new WorkerManager(config)
