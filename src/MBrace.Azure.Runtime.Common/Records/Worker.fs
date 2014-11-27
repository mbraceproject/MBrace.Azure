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

type WorkerRef (id : string, hostname : string, pid : int, pname : string, joined : DateTimeOffset) =    
    interface Nessos.MBrace.IWorkerRef with
        member __.Id = id
        member __.Type = "MBrace.Azure.Worker"
    member __.Hostname = hostname 
    member __.ProcessId = pid 
    member __.ProcessName = pname 
    member __.InitializationTime = joined 

type WorkerEntity(pk, id, hostname, pid, pname, joined, heartbeat) =
    inherit TableEntity(pk, id)
    member val Hostname : string = hostname with get, set
    member val Id = id : string with get, set
    member val ProcessId :int = pid with get, set
    member val ProcessName :string = pname with get, set
    member val InitializationTime : DateTimeOffset = joined with get, set
    member val Heartbeat : DateTimeOffset = heartbeat with get, set

    new () = new WorkerEntity(null, null, null, -1, null, Unchecked.defaultof<_>, Unchecked.defaultof<_>)

    member __.AsWorkerRef () = 
        new WorkerRef(__.Id, __.Hostname, __.ProcessId, __.ProcessName, __.InitializationTime)


type WorkerMonitor internal (config, table : string) =
    let pk = "worker"

    let current = ref None

    member __.DeclareCurrent(id : string) : Async<WorkerRef> = 
        async {
            match current.Value with 
            | Some w -> 
                return failwithf "Worker %A is active" w
            | None ->
                let ps = Process.GetCurrentProcess()
                let joined = DateTimeOffset.UtcNow
                let w = new WorkerEntity(pk, id, Dns.GetHostName(), ps.Id, ps.ProcessName, joined, joined)
                do! Table.insertOrReplace<WorkerEntity> config table w //Worker might restart but keep id.
                current := Some w
                return w.AsWorkerRef()
        }

    member __.GetWorkers(?timespan : TimeSpan) : Async<WorkerRef seq> = async {
        let timespan = defaultArg timespan <| TimeSpan.FromSeconds 60.
        let! ws = Table.queryPK<WorkerEntity> config table pk
        return ws |> Seq.filter (fun w ->  DateTimeOffset.UtcNow - w.Heartbeat < timespan)
                  |> Seq.map (fun w -> w.AsWorkerRef())
    }

    member __.Current = current.Value.Value

    member __.HeartbeatLoop(?timespan : TimeSpan) : Async<unit> = async {
        let ts = defaultArg timespan <| TimeSpan.FromSeconds(1.)
        let worker = __.Current
        worker.ETag <- "*"
        let rec loop () = async {
            worker.Heartbeat <- DateTimeOffset.UtcNow
            let! _ = Table.merge<WorkerEntity> config table worker
            do! Async.Sleep (int ts.TotalMilliseconds)
            return! loop ()
        }
        return! loop ()
    }

