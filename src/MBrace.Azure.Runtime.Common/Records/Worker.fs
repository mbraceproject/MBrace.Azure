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

type WorkerEntity(pk : string, id : string, hostname : string, pid : int, pname : string, joined : DateTime, heartbeat : DateTime) =
    inherit TableEntity(pk, id)
    member val Hostname = hostname with get, set
    member val Id = id with get, set
    member val ProcessId = pid with get, set
    member val ProcessName = pname with get, set
    member val CreationTime = joined with get, set
    member val Heartbeat = heartbeat with get, set

    interface Nessos.MBrace.IWorkerRef with
        member __.Id = id
        member __.Type = "MBrace.Azure worker"

    new () = new WorkerEntity(null, null, null, -1, null, Unchecked.defaultof<_>, Unchecked.defaultof<_>)

type WorkerMonitor private (table : string) =
    let pk = "worker"

    // TODO :
    let current = ref Unchecked.defaultof<_>

    member private __.Declare(id, hostname, pid : int, pname : string, joined : DateTime, heartbeat : DateTime)  = async {
        let e = new WorkerEntity(pk, id, hostname, pid, pname, joined, heartbeat)
        do! Table.insert<WorkerEntity> table e
        return e
    }

    member __.DeclareCurrent() = 
        let ps = Process.GetCurrentProcess()
        let joined = DateTime.UtcNow
        async {
            let! w = __.Declare(guid(), Dns.GetHostName(), ps.Id, ps.ProcessName, joined, joined)
            current := w
            return w
        }

    member __.GetWorkers() : Async<WorkerEntity seq> = async {
        return! Table.readBatch<WorkerEntity> table pk
    }

    member __.Current = current.Value

    member __.HeartbeatLoop(worker : WorkerEntity, ?timespan : TimeSpan) : Async<unit> = async {
        worker.ETag <- "*"
        let ts = defaultArg timespan <| TimeSpan.FromSeconds(1.)
        let rec loop () = async {
            worker.Heartbeat <- DateTime.UtcNow
            let! _ = Table.merge<WorkerEntity> table worker
            do! Async.Sleep (int ts.TotalMilliseconds)
            return! loop ()
        }
        return! loop ()
    }

    // TODO :
    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, context: StreamingContext): unit = 
            info.AddValue("table", table, typeof<string>)

    new(info: SerializationInfo, context: StreamingContext) =
        let table = info.GetValue("table", typeof<string>) :?> string
        new WorkerMonitor(table)

    static member Init(table : string) = new WorkerMonitor(table)
