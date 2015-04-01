namespace MBrace.Azure
open MBrace
open System

/// Represents current worker status.
type WorkerStatus = 
    /// Worker initialization.
    | Initializing
    /// Worker successfully started.
    | Running
    /// Worker stopped.
    | Stopped
    /// Unexpected worker exception.
    | Faulted of exn

    override this.ToString() =
        match this with
        | Initializing  -> "Initializing"
        | Running   -> "Running"
        | Stopped   -> "Stopped"
        | Faulted _ -> "Faulted"

    member this.Pickle () = MBrace.Azure.Runtime.Configuration.Pickler.Pickle(this)

    static member UnPickle (bytes) = MBrace.Azure.Runtime.Configuration.Pickler.UnPickle<WorkerStatus>(bytes)

/// Immutable IWorkerRef implementation for MBrace.Azure workers.
[<CustomComparison; CustomEquality>]
type WorkerRef = 
    { /// Worker/Service Id.
      Id                 : string
      ///Current worker status.
      Status             : WorkerStatus
      /// Machine's name.
      Hostname           : string
      /// Host process id.
      ProcessId          : int 
      /// Host process name.
      ProcessName        : string
      /// First worker's heartbeat time.
      InitializationTime : DateTimeOffset
      /// Last worker's heartbeat time.
      HeartbeatTime      : DateTimeOffset
      /// Worker activated ConfigurationId.
      ConfigurationId    : ConfigurationId
      /// Worker's MaxConcurrentJobCount.
      MaxJobCount        : int
      /// Workers' processor count.
      ProcessorCount     : int
      /// Current dequeued jobs.
      ActiveJobs         : int
      /// CPU usage.
      CPU                : double
      /// Total memory.
      TotalMemory        : double
      /// Memory used.
      Memory             : double
      /// Network upload (kbps).
      NetworkUp          : double
      /// Network download (kbps).
      NetworkDown        : double
      /// Runtime Version
      Version            : string } 
    
    override this.GetHashCode() = hash this.Id
    override this.Equals(other:obj) =
        match other with
        | :? WorkerRef as w -> this.Id = w.Id
        | _ -> false

    interface IComparable with
        member this.CompareTo(obj: obj): int = 
            match obj with
            | :? WorkerRef as y -> compare this.Id ((y :> IWorkerRef).Id) 
            | _ -> invalidArg "obj" "Invalid IWorkerRef instance."
        
    interface IWorkerRef with
        member this.Id = this.Id
        member this.Type = "MBrace.Azure.Worker"
        member this.ProcessorCount = this.ProcessorCount

namespace MBrace.Azure.Runtime.Info

open System
open Microsoft.WindowsAzure.Storage.Table
open MBrace.Azure.Runtime
open System.Net
open System.Threading
open MBrace
open MBrace.Azure
open Microsoft.FSharp.Linq.NullableOperators
open MBrace.Runtime
open MBrace.Azure.Runtime.Utilities

type WorkerRecord(pk, id, hostname : string, pid : Nullable<int>, pname : string, joined : DateTimeOffset, configurationHash : Nullable<int>) =
    inherit TableEntity(pk, id)
    
    member val Hostname           = hostname          with get, set
    member val Id                 = id                with get, set
    member val ProcessId          = pid               with get, set
    member val ProcessName        = pname             with get, set
    member val InitializationTime = joined            with get, set
    member val ConfigurationId    = Unchecked.defaultof<byte []> with get, set
    member val MaxJobs            = Nullable<int>()   with get, set
    member val ActiveJobs         = Nullable<int>()   with get, set
    member val ProcessorCount     = Environment.ProcessorCount with get, set
    member val CPU                = Nullable<double>() with get, set
    member val TotalMemory        = Nullable<double>() with get, set
    member val Memory             = Nullable<double>() with get, set
    member val NetworkUp          = Nullable<double>() with get, set
    member val NetworkDown        = Nullable<double>() with get, set
    member val Version            = Unchecked.defaultof<string> with get, set
    member val Status             = Unchecked.defaultof<byte []> with get, set
    new () = new WorkerRecord(null, null, null, nullableDefault, null, Unchecked.defaultof<_>, nullableDefault)

    member this.AsWorkerRef () : WorkerRef = 
        {
            Id                 = this.Id
            Status             = Configuration.Pickler.UnPickle this.Status
            Hostname           = this.Hostname
            ProcessId          = this.ProcessId.GetValueOrDefault(-1)
            ProcessName        = this.ProcessName
            InitializationTime = this.InitializationTime
            HeartbeatTime      = this.Timestamp
            ConfigurationId    = Configuration.Pickler.UnPickle this.ConfigurationId
            MaxJobCount        = this.MaxJobs.GetValueOrDefault(-1)
            ProcessorCount     = this.ProcessorCount
            ActiveJobs         = this.ActiveJobs.GetValueOrDefault(-1)
            CPU                = this.CPU.GetValueOrDefault(-1.)
            TotalMemory        = this.TotalMemory.GetValueOrDefault(-1.)
            Memory             = this.Memory.GetValueOrDefault(-1.)
            NetworkUp          = this.NetworkUp.GetValueOrDefault(-1.)
            NetworkDown        = this.NetworkDown.GetValueOrDefault(-1.)
            Version            = this.Version
        }

    member this.UpdateCounters(counters : NodePerformanceInfo) =
            this.CPU <- counters.CpuUsage
            this.TotalMemory <- counters.TotalMemory
            this.Memory <- counters.MemoryUsage
            this.NetworkUp <- counters.NetworkUsageUp
            this.NetworkDown <- counters.NetworkUsageDown

[<AutoSerializable(false)>]
type WorkerManager private (config : ConfigurationId, logger : ICloudLogger) =
    static let pk = "WorkerRef"
    let table = config.RuntimeTable

    let perfMon = lazy new PerformanceMonitor()
    let mutable current = None : WorkerRecord option
    let mutable sendHeartBeats = true
    let runningPickle = WorkerStatus.Running.Pickle()

    member this.Current : WorkerRecord = 
        match current with
        | Some c -> c
        | None -> failwith "No worker registered."
    
    member this.SetJobCountLocal(jobCount) =
        current.Value.ActiveJobs <- nullable jobCount

    member this.SetCurrentAsRunning() =
        current.Value.Status <- runningPickle

    member this.HeartbeatLoop(timespan : TimeSpan) : Async<unit> = async {
        let maxTimespan = TimeSpan.FromMinutes(10.) // http://blogs.msdn.com/b/kwill/archive/2011/05/05/windows-azure-role-architecture.aspx
        if timespan > maxTimespan then raise(ArgumentOutOfRangeException("timespan", "Max TimeSpan of 10 minutes allowed."))
        
        let rec loop fault (currentTimespan : TimeSpan) = async {
            let! fault, currentTimespan = async {
                try
                    let counters = perfMon.Value.GetCounters()
                    current.Value.UpdateCounters(counters)
                    current.Value.ETag <- "*"
                    let! e = Table.merge<WorkerRecord> config table current.Value
                    current <- Some e
                    if fault then
                        let newTimeSpan = TimeSpan.FromTicks(timespan.Ticks / 2L)
                        logger.Logf "Decreasing timespan to %A" newTimeSpan
                        return false, newTimeSpan
                    else
                        return false, currentTimespan
                with ex ->
                    logger.Logf "Failed to give heartbeat %A" ex
                    let newTimeSpan = min (TimeSpan.FromTicks(timespan.Ticks * 2L)) maxTimespan
                    logger.Logf "Increasing timespan to %A" newTimeSpan
                    return true, newTimeSpan

                }
            do! Async.Sleep (int currentTimespan.TotalMilliseconds)
            if sendHeartBeats then return! loop fault currentTimespan
        }
        logger.Logf "Starting heartbeat loop with interval %A" timespan
        return! loop false timespan
    }

    member this.RegisterLocal(workerId) =
        async {
            let! record = Table.read<WorkerRecord> config table pk workerId
            current <- Some record
        }

    member this.RegisterCurrent(workerId : string, ?maxJobs) : Async<unit> = 
        async {
            match current with 
            | Some w -> 
                return failwithf "Worker %A is active" w
            | None ->
                perfMon.Value.Start()
                let ps = Diagnostics.Process.GetCurrentProcess()
                let joined = DateTimeOffset.UtcNow
                let w = new WorkerRecord(pk, workerId, Dns.GetHostName(), nullable ps.Id, ps.ProcessName, joined, nullable(hash config))
                w.UpdateCounters(perfMon.Value.GetCounters())
                w.ActiveJobs <- nullable 0
                w.Status <- WorkerStatus.Initializing.Pickle()
                w.Version <- ReleaseInfo.localVersion.ToString(4)
                w.MaxJobs <- match maxJobs with None -> nullableDefault | Some mj -> nullable mj
                w.ConfigurationId <- Configuration.Pickler.Pickle config
                do! Table.insertOrReplace<WorkerRecord> config table w //Worker might restart but keep id.
                current <- Some w
        }

    member this.SetCurrentAsStopped () : Async<unit> = 
        async {
            (perfMon.Value :> IDisposable).Dispose()
            sendHeartBeats <- false
            do! this.SetWorkerStopped(this.Current)
        }

    member this.SetWorkerStopped(worker : WorkerRecord) : Async<unit> =
        async {
            worker.Status <- WorkerStatus.Stopped.Pickle()
            let! _ = Table.replace config table worker
            return ()
        }

    member this.SetCurrentAsFaulted(ex : Exception) =
        current.Value.Status <- (WorkerStatus.Faulted ex).Pickle()
        
    member this.DeleteWorkerRecord(workerId : string) : Async<unit> =
        async {
            let! record = this.GetWorker(workerId)
            do! Table.delete config table record
        }

    member this.GetWorker(workerId : string) = 
        async {
            return! Table.read<WorkerRecord> config table pk workerId
        }

    member this.GetWorkers(timespan : TimeSpan, showStarting : bool, showInactive : bool, showFaulted : bool) : Async<WorkerRecord seq> = async {
        let! ws = Table.queryPK<WorkerRecord> config table pk
        // TODO : Make timespan part of the query?
        return ws |> Seq.filter (fun w -> DateTimeOffset.UtcNow - w.Timestamp < timespan)
                  |> Seq.filter (fun w ->
                        match Configuration.Pickler.UnPickle<WorkerStatus> w.Status with
                        | Running -> true
                        | Initializing -> showStarting
                        | Stopped -> showInactive
                        | Faulted _ -> showFaulted )
    }

    member this.GetWorkerRefs(timespan : TimeSpan, showStarting : bool, showInactive : bool, showFaulted : bool) : Async<WorkerRef seq> =
        async {
            let! wr = this.GetWorkers(timespan, showStarting, showInactive, showFaulted)
            return wr |> Seq.map (fun w -> w.AsWorkerRef())
        }

    static member Create(config : ConfigurationId, logger : ICloudLogger) = new WorkerManager(config, logger)

    /// Unrecoverable worker faults.
    static member SetFaulted (config : ConfigurationId, workerId : string, ex : exn) : Async<unit> =
        async {
            let ps = Diagnostics.Process.GetCurrentProcess()
            let joined = DateTimeOffset.UtcNow
            let w = new WorkerRecord(pk, workerId, Dns.GetHostName(), nullable ps.Id, ps.ProcessName, joined, nullable(hash config))
            w.Status <- (Faulted ex).Pickle()
            w.ETag <- "*"
            do! Table.insertOrReplace config config.RuntimeTable w
                |> Async.Ignore
        }