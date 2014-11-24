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
        member __.Type = "MBrace.Azure.Worker"
    member __.Hostname = hostname 
    member __.ProcessId = pid 
    member __.ProcessName = pname 
    member __.InitializationTime = joined 

type WorkerEntity(pk : string, id : string, hostname : string, pid : int, pname : string, joined : DateTime, heartbeat : DateTime) =
    inherit TableEntity(pk, id)
    member val Hostname = hostname with get, set
    member val Id = id with get, set
    member val ProcessId = pid with get, set
    member val ProcessName = pname with get, set
    member val InitializationTime = joined with get, set
    member val Heartbeat = heartbeat with get, set

    new () = new WorkerEntity(null, null, null, -1, null, Unchecked.defaultof<_>, Unchecked.defaultof<_>)

    member __.AsWorkerRef () = 
        new WorkerRef(__.Id, __.Hostname, __.ProcessId, __.ProcessName, __.InitializationTime)


type WorkerMonitor internal (table : string) =
    let pk = "worker"

    let current = ref None

    member __.DeclareCurrent(id : string) : Async<WorkerRef> = 
        async {
            match current.Value with 
            | Some w -> 
                return failwithf "Worker %A is active" w
            | None ->
                let ps = Process.GetCurrentProcess()
                let joined = DateTime.UtcNow
                let w = new WorkerEntity(pk, id, Dns.GetHostName(), ps.Id, ps.ProcessName, joined, joined)
                do! Table.insert<WorkerEntity> table w
                current := Some w
                return w.AsWorkerRef()
        }

    member __.GetWorkers(?timespan : TimeSpan) : Async<WorkerRef seq> = async {
        let timespan = defaultArg timespan <| TimeSpan.FromSeconds 30.
        let now = DateTime.UtcNow
        let! ws = Table.readBatch<WorkerEntity> table pk
        return ws |> Seq.filter (fun w -> now - w.Heartbeat < timespan)
                  |> Seq.map (fun w -> w.AsWorkerRef())
    }

    member __.Current = current.Value.Value

    member __.HeartbeatLoop(?timespan : TimeSpan) : Async<unit> = async {
        let ts = defaultArg timespan <| TimeSpan.FromSeconds(1.)
        let worker = __.Current
        worker.ETag <- "*"
        let rec loop () = async {
            worker.Heartbeat <- DateTime.UtcNow
            let! _ = Table.merge<WorkerEntity> table worker
            do! Async.Sleep (int ts.TotalMilliseconds)
            return! loop ()
        }
        return! loop ()
    }

