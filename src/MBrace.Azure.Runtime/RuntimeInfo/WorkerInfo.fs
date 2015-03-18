namespace MBrace.Azure
open MBrace
open System

/// Immutable IWorkerRef implementation for MBrace.Azure workers.
[<CustomComparison; CustomEquality>]
type WorkerRef = 
    { /// Worker/Service Id.
      Id : string
      /// Machine's name.
      Hostname : string
      /// Host process id.
      ProcessId : int 
      /// Host process name.
      ProcessName : string
      /// First worker's heartbeat time.
      InitializationTime : DateTimeOffset
      /// Last worker's heartbeat time.
      HeartbeatTime : DateTimeOffset
      /// Hash of worker's activated ConfigurationId.
      ConfigurationHash : int
      /// Worker's MaxConcurrentJobCount.
      MaxJobCount : int
      /// Workers' processor count.
      ProcessorCount : int
      /// Current dequeued jobs.
      ActiveJobs  : int
      /// CPU usage.
      CPU : double
      /// Total memory.
      TotalMemory : double
      /// Memory used.
      Memory : double
      /// Network upload (kbps).
      NetworkUp : double
      /// Network download (kbps).
      NetworkDown : double } 
    
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
    member val ConfigurationHash  = configurationHash with get, set
    member val IsActive           = Nullable<bool>()  with get, set
    member val MaxJobs            = Nullable<int>()   with get, set
    member val ActiveJobs         = Nullable<int>()   with get, set
    member val ProcessorCount     = Environment.ProcessorCount with get, set
    member val CPU                = Nullable<double>() with get, set
    member val TotalMemory        = Nullable<double>() with get, set
    member val Memory             = Nullable<double>() with get, set
    member val NetworkUp          = Nullable<double>() with get, set
    member val NetworkDown        = Nullable<double>() with get, set

    new () = new WorkerRecord(null, null, null, nullableDefault, null, Unchecked.defaultof<_>, nullableDefault)

    member this.AsWorkerRef () : WorkerRef = 
        {
            Id = this.Id
            Hostname = this.Hostname
            ProcessId = this.ProcessId.Value
            ProcessName = this.ProcessName
            InitializationTime = this.InitializationTime
            HeartbeatTime = this.Timestamp
            ConfigurationHash = this.ConfigurationHash.Value
            MaxJobCount = this.MaxJobs.Value
            ProcessorCount = this.ProcessorCount
            ActiveJobs = this.ActiveJobs.Value
            CPU = this.CPU.Value
            TotalMemory = this.TotalMemory.Value
            Memory = this.Memory.Value
            NetworkUp = this.NetworkUp.Value
            NetworkDown = this.NetworkDown.Value
        }

    member this.UpdateCounters(counters : NodePerformanceInfo) =
            this.CPU <- counters.CpuUsage
            this.TotalMemory <- counters.TotalMemory
            this.Memory <- counters.MemoryUsage
            this.NetworkUp <- counters.NetworkUsageUp
            this.NetworkDown <- counters.NetworkUsageDown

[<AutoSerializable(false)>]
type WorkerManager private (config : ConfigurationId, logger : ICloudLogger) =
    let pk = "WorkerRef"
    let table = config.RuntimeTable

    let perfMon = lazy new PerformanceMonitor()
    let mutable active = false
    let current = ref None

    member this.Current : WorkerRecord = 
        match current.Value with
        | Some c -> c
        | None -> failwith "No worker registered."
    
    member this.SetJobCountLocal(jobCount) =
        current.Value.Value.ActiveJobs <- nullable jobCount

    member this.HeartbeatLoop(?timespan : TimeSpan) : Async<unit> = async {
        let ts = defaultArg timespan <| TimeSpan.FromSeconds(1.)
        active <- true
        let worker = this.Current
        let rec loop () = async {
            let counters = perfMon.Value.GetCounters()
            worker.UpdateCounters(counters)
            worker.IsActive <- nullable true
            //worker.ActiveJobs <- nullableDefault
            worker.ETag <- "*"
            let! e = Table.merge<WorkerRecord> config table worker
            current := Some e
            do! Async.Sleep (int ts.TotalMilliseconds)
            if active then return! loop ()
        }
        return! loop ()
    }

    member this.RegisterLocal(workerId) =
        async {
            let! record = Table.read<WorkerRecord> config table pk workerId
            current := Some record
        }

    member this.RegisterCurrent(workerId : string, ?maxJobs) : Async<unit> = 
        async {
            match current.Value with 
            | Some w -> 
                return failwithf "Worker %A is active" w
            | None ->
                perfMon.Value.Start()
                let ps = Diagnostics.Process.GetCurrentProcess()
                let joined = DateTimeOffset.UtcNow
                let w = new WorkerRecord(pk, workerId, Dns.GetHostName(), nullable ps.Id, ps.ProcessName, joined, nullable(hash config))
                w.UpdateCounters(perfMon.Value.GetCounters())
                w.ActiveJobs <- nullable 0
                w.IsActive <- nullable true
                w.MaxJobs <- match maxJobs with None -> nullableDefault | Some mj -> nullable mj
                do! Table.insertOrReplace<WorkerRecord> config table w //Worker might restart but keep id.
                current := Some w
        }

    member this.UnregisterCurrent () : Async<unit> = 
        async {
            active <- false
            (perfMon.Value :> IDisposable).Dispose()
            do! this.SetInactiveWorker(this.Current)
        }

    member this.SetInactiveWorker(worker : WorkerRecord) : Async<unit> =
        async {
            worker.IsActive <- nullable false
            let! _ = Table.replace config table worker
            return ()
        }

    member this.DeleteWorkerRecord(workerId : string) : Async<unit> =
        async {
            let! record = this.GetWorker(workerId)
            do! Table.delete config table record
        }

    member this.GetWorker(workerId : string) = 
        async {
            return! Table.read<WorkerRecord> config table pk workerId
        }

    member this.GetWorkers(?timespan : TimeSpan, ?showInactive : bool) : Async<WorkerRecord seq> = async {
        let timespan = defaultArg timespan <| TimeSpan.FromMinutes 5.
        let showInactive = defaultArg showInactive false
        // TODO : Make showInactive part of the Table query
        let! ws = Table.queryPK<WorkerRecord> config table pk
        return ws |> Seq.filter (fun w -> DateTimeOffset.UtcNow - w.Timestamp < timespan)
                  |> Seq.filter (fun w -> showInactive || (w.IsActive.HasValue && w.IsActive.Value))
    }

    member this.GetWorkerRefs(?timespan : TimeSpan, ?showInactive : bool) : Async<WorkerRef seq> =
        async {
            let! wr = this.GetWorkers(?timespan = timespan, ?showInactive = showInactive)
            return wr |> Seq.map (fun w -> w.AsWorkerRef())
        }

    static member Create(config : ConfigurationId, logger : ICloudLogger) = new WorkerManager(config, logger)
