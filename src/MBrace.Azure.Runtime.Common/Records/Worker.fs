namespace Nessos.MBrace.Azure.Runtime.Common

open System
open System.Runtime.Serialization
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging
open Nessos.MBrace.Azure.Runtime
open System.Net
open System.Diagnostics
open Nessos.MBrace

type WorkerRef (id : string, hostname : string, pid : int, pname : string, joined : DateTime) =    
    interface Nessos.MBrace.IWorkerRef with
        member __.Id = id
        member __.Type = "MBrace.Azure worker"
    member __.Hostname = hostname 
    member __.ProcessId = pid 
    member __.ProcessName = pname 
    member __.CreationTime = joined 

type WorkerEntity(pk : string, id : string, hostname : string, pid : int, pname : string, joined : DateTime, heartbeat : DateTime) =
    inherit TableEntity(pk, id)
    member val Hostname = hostname with get, set
    member val Id = id with get, set
    member val ProcessId = pid with get, set
    member val ProcessName = pname with get, set
    member val CreationTime = joined with get, set
    member val Heartbeat = heartbeat with get, set

    new () = new WorkerEntity(null, null, null, -1, null, Unchecked.defaultof<_>, Unchecked.defaultof<_>)

    member __.AsWorkerRef () = new WorkerRef(id, hostname, pid, pname, joined)


type WorkerMonitor private (table : string) =
    let pk = "worker"

    // TODO :
    static let monitor : WorkerMonitor option ref = ref None 
    let current = ref None

    static member Activated = monitor.Value.Value

    static member Activate(table : string) = 
        if monitor.Value.IsSome then monitor.Value.Value
        else
            let m = new WorkerMonitor(table)
            monitor := Some m
            m
    member private __.Declare(id, hostname, pid : int, pname : string, joined : DateTime, heartbeat : DateTime)  = async {
        let e = new WorkerEntity(pk, id, hostname, pid, pname, joined, heartbeat)
        do! Table.insert<WorkerEntity> table e
        return e
    }

    member __.DeclareCurrent(id : string) = 
        let ps = Process.GetCurrentProcess()
        let joined = DateTime.UtcNow
        async {
            let! w = __.Declare(id, Dns.GetHostName(), ps.Id, ps.ProcessName, joined, joined)
            current := Some w
            return w
        }

    member __.GetWorkers(?timespan : TimeSpan) : Async<WorkerEntity seq> = async {
        let timespan = defaultArg timespan <| TimeSpan.FromSeconds 30.
        let now = DateTime.UtcNow
        let! ws = Table.readBatch<WorkerEntity> table pk
        return ws |> Seq.filter (fun w -> now - w.Heartbeat < timespan)
    }

    member __.HeartbeatLoop(worker : WorkerEntity, ?timespan : TimeSpan) : Async<unit> = async {
        let ts = defaultArg timespan <| TimeSpan.FromSeconds(1.)
        worker.ETag <- "*"
        let rec loop () = async {
            worker.Heartbeat <- DateTime.UtcNow
            let! _ = Table.merge<WorkerEntity> table worker
            do! Async.Sleep (int ts.TotalMilliseconds)
            return! loop ()
        }
        return! loop ()
    }

    member __.Current = current.Value.Value
