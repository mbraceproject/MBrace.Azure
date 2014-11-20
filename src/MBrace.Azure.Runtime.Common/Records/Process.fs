namespace Nessos.MBrace.Azure.Runtime.Common

open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Nessos.MBrace
open Nessos.MBrace.Azure.Runtime
open System
open System.Diagnostics
open System.Net
open System.Runtime.Serialization

type ProcessState = 
    | Initialized
    | Completed
    override this.ToString() = 
        match this with
        | Initialized -> "Initialized"
        | Completed -> "Completed"

type ProcessEntity(pk : string, pid : string) = 
    inherit TableEntity(pk, pid)
    member val ProcessId = pid with get, set
    member val State : string = null with get, set
    member val Created : DateTime = Unchecked.defaultof<_> with get, set
    member val Completed : DateTime = Unchecked.defaultof<_> with get, set
    member val ResultUri : string = null with get, set
    new() = new ProcessEntity(null, null)

type ProcessMonitor private (table : string) = 
    let pk = "process"

    static let monitor : ProcessMonitor option ref = ref None
    static member Activated = monitor.Value.Value
    
    static member Activate(table : string) = 
        if monitor.Value.IsSome then monitor.Value.Value
        else 
            let m = new ProcessMonitor(table)
            monitor := Some m
            m
    
    member this.Create(pid : string) = async { 
        let e = new ProcessEntity(pk, pid, State = ProcessState.Initialized.ToString(), Created = DateTime.UtcNow)
        do! Table.insert<ProcessEntity> table e
        return e
    }

    member this.Complete(pid : string, result : string) = async {
        let! e = Table.read<ProcessEntity> table pk pid
        e.State <- ProcessState.Completed.ToString()
        e.Completed <- DateTime.UtcNow
        e.ResultUri <- result
        let! e' = Table.merge table e
        return ()
    }

    member this.GetProcess(pid : string) = async {
        return! Table.read<ProcessEntity> table pid String.Empty
    }

    member this.GetProcesses () = async {
        return! Table.readBatch<ProcessEntity> table pk
    }