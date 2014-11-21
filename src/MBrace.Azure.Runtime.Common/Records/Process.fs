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
    | Killed
    | Completed
    override this.ToString() = 
        match this with
        | Initialized -> "Initialized"
        | Killed -> "Killed"
        | Completed -> "Completed"

type ProcessEntity(pk : string, pid : string, state, created, completed, resultUri) = 
    inherit TableEntity(pk, pid)
    member val ProcessId = pid with get, set
    member val State : string = state with get, set
    member val Created : DateTime = created with get, set
    member val Completed : DateTime = completed with get, set
    member val ResultUri : string = resultUri with get, set
    new() = new ProcessEntity(null, null, null, Unchecked.defaultof<_>, Unchecked.defaultof<_>, null)

type ProcessMonitor internal (table : string) = 
    let pk = "process"
    
    member this.Create(pid : string) = async { 
        let now = DateTime.UtcNow
        let e = new ProcessEntity(pk, pid, ProcessState.Initialized.ToString(), now, now, null)
        do! Table.insert<ProcessEntity> table e
        return ()
    }

    member this.SetKilled(pid : string) = async {
        let! e = Table.read<ProcessEntity> table pk pid
        e.State <- ProcessState.Killed.ToString()
        e.Completed <- DateTime.UtcNow
        let! e' = Table.merge table e
        return ()
    }

    member this.SetCompleted(pid : string, result : string) = async {
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