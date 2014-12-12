namespace Nessos.MBrace.Azure.Runtime.Common

open System
open System.Runtime.Serialization
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging
open Nessos.MBrace.Azure.Runtime
open System.Net
open Nessos.MBrace

type WorkerRef (id : string, hostname : string, pid : int, pname : string, joined : DateTimeOffset, heartbeat : DateTimeOffset) =    
    interface Nessos.MBrace.IWorkerRef with
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
    
    member val ActiveTasks : Nullable<int64> = Unchecked.defaultof<_> with get, set
    member val CompletedTasks : Nullable<int64> = Unchecked.defaultof<_> with get, set

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
    let perfMon = new PerformanceMonitor()
    let inc (x : Nullable<int64>) = Nullable<_>(if x.HasValue then x.Value + 1L else 1L)
    let dec (x : Nullable<int64>) = Nullable<_>(if x.HasValue then x.Value - 1L else -1L)

    static member Create(config : Configuration) = new WorkerMonitor(config.ConfigurationId, config.DefaultTableOrContainer)

    member __.DeclareCurrent(id : string) : Async<WorkerRef> = 
        async {
            match current.Value with 
            | Some w -> 
                return failwithf "Worker %A is active" w
            | None ->
                perfMon.Start()
                let ps = Diagnostics.Process.GetCurrentProcess()
                let joined = DateTimeOffset.UtcNow
                let w = new WorkerRecord(pk, id, Dns.GetHostName(), ps.Id, ps.ProcessName, joined)
                w.UpdateCounters(perfMon.GetCounters())
                do! Table.insertOrReplace<WorkerRecord> config table w //Worker might restart but keep id.
                current := Some w
                return w.AsWorkerRef()
        }

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

    member __.Current = current.Value.Value

    member __.AddActiveTask () = async {
        let! e = Table.transact<WorkerRecord> config table pk __.Current.RowKey (fun e -> e.ActiveTasks <- inc e.ActiveTasks)
        return ()
    }

    member __.AddCompletedTask () = async {
        let! e = Table.transact<WorkerRecord> config table pk __.Current.RowKey 
                    (fun e -> e.ActiveTasks <- dec e.ActiveTasks
                              e.CompletedTasks <- inc e.CompletedTasks)
        return ()        
    }

    member __.HeartbeatLoop(?timespan : TimeSpan) : Async<unit> = async {
        let ts = defaultArg timespan <| TimeSpan.FromSeconds(1.)
        let worker = __.Current
        let rec loop () = async {
            let counters = perfMon.GetCounters()
            worker.UpdateCounters(counters)
            worker.ETag <- "*"
            let! e = Table.merge<WorkerRecord> config table worker
            current := Some e
            do! Async.Sleep (int ts.TotalMilliseconds)
            return! loop ()
        }
        return! loop ()
    }

