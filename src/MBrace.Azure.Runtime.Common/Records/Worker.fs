namespace MBrace.Azure
open MBrace
open System

/// IWorkerRef implementation for MBrace.Azure workers.
type WorkerRef internal (id : string, hostname : string, pid : int, pname : string, joined : DateTimeOffset, heartbeat : DateTimeOffset, configurationHash, maxJobCount, processorCount) =    
    /// Worker/Service Id.
    member __.Id = id
    /// Machine's name.
    member __.Hostname = hostname 
    /// Host process id.
    member __.ProcessId = pid 
    /// Host process name.
    member __.ProcessName = pname 
    /// First worker's heartbeat time.
    member __.InitializationTime = joined 
    /// Last worker's heatbeat time.
    member __.HeartbeatTime = heartbeat
    /// Hash of worker's activated ConfigurationId.
    member __.ConfigurationHash = configurationHash
    /// Worker's MaxConcurrentJobCount.
    member __.MaxJobCount = maxJobCount
    /// Workers' processor count.
    member __.ProcessorCount = processorCount
    override __.GetHashCode() = hash id
    override __.Equals(other:obj) =
        match other with
        | :? WorkerRef as w -> id = w.Id
        | _ -> false

    interface IWorkerRef with
        member x.CompareTo(obj: obj): int = 
            match obj with
            | :? WorkerRef as y -> compare id ((y :> IWorkerRef).Id) 
            | _ -> invalidArg "obj" "Invalid IWorkerRef instance."
        member __.Id = id
        member __.Type = "MBrace.Azure.Worker"

namespace MBrace.Azure.Runtime.Common

open System
open Microsoft.WindowsAzure.Storage.Table
open MBrace.Azure.Runtime
open System.Net
open System.Threading
open MBrace
open MBrace.Azure

type WorkerRecord(pk, id, hostname, pid, pname, joined, configurationHash) =
    inherit TableEntity(pk, id)
    member val Hostname : string = hostname with get, set
    member val Id = id : string with get, set
    member val ProcessId :int = pid with get, set
    member val ProcessName :string = pname with get, set
    member val InitializationTime : DateTimeOffset = joined with get, set
    member val IsActive : bool = true with get, set
    member val ConfigurationHash : int = configurationHash with get, set
    member val MaxJobs = 0 with get, set
    member val ActiveJobs = 0 with get, set

    member val ProcessorCount = System.Environment.ProcessorCount with get, set
    member val CPU = Unchecked.defaultof<_> with get, set
    member val TotalMemory = Unchecked.defaultof<_> with get, set
    member val Memory = Unchecked.defaultof<_> with get, set
    member val NetworkUp = Unchecked.defaultof<_> with get, set
    member val NetworkDown = Unchecked.defaultof<_> with get, set

    new () = new WorkerRecord(null, null, null, -1, null, Unchecked.defaultof<_>, -1)

    member __.AsWorkerRef () = 
        new WorkerRef(
            __.Id,
            __.Hostname, 
            __.ProcessId, 
            __.ProcessName, 
            __.InitializationTime, 
            __.Timestamp, 
            __.ConfigurationHash,
            __.MaxJobs,
            __.ProcessorCount)

    member __.UpdateCounters(counters : NodePerformanceInfo) =
            __.CPU <- counters.CpuUsage
            __.TotalMemory <- counters.TotalMemory
            __.Memory <- counters.MemoryUsage
            __.NetworkUp <- counters.NetworkUsageUp
            __.NetworkDown <- counters.NetworkUsageDown

type WorkerManager private (config : ConfigurationId) =
    let pk = "WorkerRef"
    let table = config.RuntimeTable

    let current = ref None
    let perfMon = lazy new PerformanceMonitor()
    let mutable active = false
    let mutable activeJobs = 0

    static member Create(config : ConfigurationId) = new WorkerManager(config)

    member __.ActiveJobs = activeJobs
    
    member val MaxJobs = 0 with get, set

    member __.IncrementJobCount () = Interlocked.Increment(&activeJobs) |> ignore

    member __.DecrementJobCount () = Interlocked.Decrement(&activeJobs) |> ignore

    member __.Current : WorkerRecord = current.Value.Value

    member this.HeartbeatLoop(?timespan : TimeSpan) : Async<unit> = async {
        let ts = defaultArg timespan <| TimeSpan.FromSeconds(1.)
        let worker = this.Current
        worker.MaxJobs <- this.MaxJobs
        active <- true
        let rec loop () = async {
            let counters = perfMon.Value.GetCounters()
            worker.UpdateCounters(counters)
            worker.ActiveJobs <- activeJobs
            worker.IsActive <- true
            worker.ETag <- "*"
            let! e = Table.merge<WorkerRecord> config table worker
            current := Some e
            do! Async.Sleep (int ts.TotalMilliseconds)
            if active then return! loop ()
        }
        return! loop ()
    }

    member this.RegisterCurrent(workerId : string) : Async<WorkerRef> = 
        async {
            match current.Value with 
            | Some w -> 
                return failwithf "Worker %A is active" w
            | None ->
                perfMon.Value.Start()
                let ps = Diagnostics.Process.GetCurrentProcess()
                let joined = DateTimeOffset.UtcNow
                let w = new WorkerRecord(pk, workerId, Dns.GetHostName(), ps.Id, ps.ProcessName, joined, hash config)
                w.UpdateCounters(perfMon.Value.GetCounters())
                do! Table.insertOrReplace<WorkerRecord> config table w //Worker might restart but keep id.
                current := Some w
                return w.AsWorkerRef()
        }

    member __.UnregisterCurrent () : Async<unit> = 
        async {
            active <- false
            (perfMon.Value :> IDisposable).Dispose()
            do! __.SetInactiveWorker(__.Current)
        }

    member __.SetInactiveWorker(worker : WorkerRecord) : Async<unit> =
        async {
            worker.IsActive <- false
            let! _ = Table.replace config table worker
            return ()
        }

    member __.DeleteWorkerRecord(workerId : string) : Async<unit> =
        async {
            let! record = __.GetWorker(workerId)
            do! Table.delete config table record
        }

    member __.GetWorker(workerId : string) = 
        async {
            return! Table.read<WorkerRecord> config table pk workerId
        }

    member __.GetWorkers(?timespan : TimeSpan, ?showInactive : bool) : Async<WorkerRecord seq> = async {
        let timespan = defaultArg timespan <| TimeSpan.FromMinutes 5.
        let showInactive = defaultArg showInactive false
        // TODO : Make showInactive part of the Table query
        let! ws = Table.queryPK<WorkerRecord> config table pk
        return ws |> Seq.filter (fun w -> DateTimeOffset.UtcNow - w.Timestamp < timespan)
                  |> Seq.filter (fun w -> showInactive || w.IsActive)
    }

    member __.GetWorkerRefs(?timespan : TimeSpan, ?showInactive : bool) : Async<WorkerRef seq> =
        async {
            let! wr = __.GetWorkers(?timespan = timespan, ?showInactive = showInactive)
            return wr |> Seq.map (fun w -> w.AsWorkerRef())
        }

