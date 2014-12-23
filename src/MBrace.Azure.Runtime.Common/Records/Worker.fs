namespace Nessos.MBrace.Azure.Runtime.Common

open System
open Microsoft.WindowsAzure.Storage.Table
open Nessos.MBrace.Azure.Runtime
open System.Net
open System.Threading
open Nessos.MBrace

type WorkerRef (id : string, hostname : string, pid : int, pname : string, joined : DateTimeOffset, heartbeat : DateTimeOffset) =    
    interface IWorkerRef with
        member x.CompareTo(obj: obj): int = 
            match obj with
            | :? WorkerRef as y -> compare id ((y :> IWorkerRef).Id) 
            | _ -> invalidArg "obj" "Invalid IWorkerRef instance."
        member __.Id = id
        member __.Type = "MBrace.Azure.Worker"
    member __.Hostname = hostname 
    member __.ProcessId = pid 
    member __.ProcessName = pname 
    member __.InitializationTime = joined 
    member __.HeartbeatTime = heartbeat

type WorkerRecord(pk, id, hostname, pid, pname, joined) =
    inherit TableEntity(pk, id)
    member val Hostname : string = hostname with get, set
    member val Id = id : string with get, set
    member val ProcessId :int = pid with get, set
    member val ProcessName :string = pname with get, set
    member val InitializationTime : DateTimeOffset = joined with get, set
    
    member val MaxTasks = 0 with get, set
    member val ActiveTasks = 0 with get, set

    member val ProcessorCount = System.Environment.ProcessorCount with get, set
    member val CPU = Unchecked.defaultof<_> with get, set
    member val TotalMemory = Unchecked.defaultof<_> with get, set
    member val Memory = Unchecked.defaultof<_> with get, set
    member val NetworkUp = Unchecked.defaultof<_> with get, set
    member val NetworkDown = Unchecked.defaultof<_> with get, set

    new () = new WorkerRecord(null, null, null, -1, null, Unchecked.defaultof<_>)

    member __.AsWorkerRef () = 
        new WorkerRef(__.Id, __.Hostname, __.ProcessId, __.ProcessName, __.InitializationTime, __.Timestamp)

    member __.UpdateCounters(counters : NodePerformanceInfo) =
            __.CPU <- counters.CpuUsage
            __.TotalMemory <- counters.TotalMemory
            __.Memory <- counters.MemoryUsage
            __.NetworkUp <- counters.NetworkUsageUp
            __.NetworkDown <- counters.NetworkUsageDown

type WorkerMonitor private (config, table : string) =
    let pk = "worker"

    let current = ref None
    let perfMon = lazy new PerformanceMonitor()

    let mutable activeTasks = 0

    static member Create(config : Configuration) = new WorkerMonitor(config.ConfigurationId, config.DefaultTableOrContainer)

    member this.DeclareCurrent(id : string) : Async<WorkerRef> = 
        async {
            match current.Value with 
            | Some w -> 
                return failwithf "Worker %A is active" w
            | None ->
                perfMon.Value.Start()
                let ps = Diagnostics.Process.GetCurrentProcess()
                let joined = DateTimeOffset.UtcNow
                let w = new WorkerRecord(pk, id, Dns.GetHostName(), ps.Id, ps.ProcessName, joined)
                w.UpdateCounters(perfMon.Value.GetCounters())
                do! Table.insertOrReplace<WorkerRecord> config table w //Worker might restart but keep id.
                current := Some w
                return w.AsWorkerRef()
        }

    member __.Current = current.Value.Value

    member this.HeartbeatLoop(?timespan : TimeSpan) : Async<unit> = async {
        let ts = defaultArg timespan <| TimeSpan.FromSeconds(1.)
        let worker = this.Current
        worker.MaxTasks <- this.MaxTasks
        let rec loop () = async {
            let counters = perfMon.Value.GetCounters()
            worker.UpdateCounters(counters)
            worker.ActiveTasks <- activeTasks
            worker.ETag <- "*"
            let! e = Table.merge<WorkerRecord> config table worker
            current := Some e
            do! Async.Sleep (int ts.TotalMilliseconds)
            return! loop ()
        }
        return! loop ()
    }

    member __.ActiveTasks = activeTasks
    
    member val MaxTasks = 0 with get, set

    member __.IncrementTaskCount () = Interlocked.Increment(&activeTasks) |> ignore

    member __.DecrementTaskCount () = Interlocked.Decrement(&activeTasks) |> ignore

    member __.GetWorkers(?timespan : TimeSpan) : Async<WorkerRecord seq> = async {
        let timespan = defaultArg timespan <| TimeSpan.FromMinutes 5.
        let! ws = Table.queryPK<WorkerRecord> config table pk
        return ws |> Seq.filter (fun w -> DateTimeOffset.UtcNow - w.Timestamp < timespan)
    }

    member __.GetWorkerRefs(?timespan : TimeSpan) : Async<WorkerRef seq> =
        async {
            let! wr = __.GetWorkers(?timespan = timespan)
            return wr |> Seq.map (fun w -> w.AsWorkerRef())
        }

