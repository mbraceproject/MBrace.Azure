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
open Nessos.Vagrant

type ProcessState = 
    | Running
    | Killed
    | Completed
    override this.ToString() = 
        match this with
        | Running -> "Running"
        | Killed -> "Killed"
        | Completed -> "Completed"

type ProcessRecord(pk, pid, pname, cancellationUri, state, createdt, completedt, completed, resultUri, ty, typeName, deps) = 
    inherit TableEntity(pk, pid)
    member val Id  : string = pid with get, set
    member val Name : string = pname with get, set
    member val State : string = state with get, set
    member val InitializationTime : DateTimeOffset = createdt with get, set
    member val CompletionTime : DateTimeOffset = completedt with get, set
    member val Completed : bool = completed with get, set
    member val ResultUri : string = resultUri with get, set
    member val CancellationUri : string = cancellationUri with get, set
    member val TypeName : string = typeName with get, set
    member val Type : byte [] = ty with get, set
    member val Dependencies : byte [] = deps with get, set
    member __.UnpickleType () = Configuration.Pickler.UnPickle<Type> __.Type
    member __.UnpickleDependencies () = Configuration.Pickler.UnPickle<AssemblyId list> __.Dependencies
    new () = new ProcessRecord(null, null, null, null, null, Unchecked.defaultof<_>, Unchecked.defaultof<_>, false, null, null, null, null)

type ProcessMonitor internal (config, table : string) = 
    let pk = "process"
    
    member this.CreateRecord(pid : string, name, ty : Type, deps : AssemblyId list, ctsUri, resultUri) = async { 
        let now = DateTimeOffset.UtcNow
        let pickledTy = Configuration.Pickler.Pickle(ty)
        let deps = Configuration.Pickler.Pickle(deps)
        let e = new ProcessRecord(pk, pid, name, ctsUri, string ProcessState.Running, now, now, false, resultUri, pickledTy, ty.Name, deps)
        do! Table.insert<ProcessRecord> config table e
        return e
    }

    member this.SetKilled(pid : string) = async {
        let! e = Table.read<ProcessRecord> config table pk pid
        e.State <- string ProcessState.Killed
        e.CompletionTime <- DateTimeOffset.Now
        e.Completed <- true
        let! e' = Table.merge config table e
        return ()
    }

    member this.SetCompleted(pid : string) = async {
        let! e = Table.read<ProcessRecord> config table pk pid
        e.State <- string ProcessState.Completed
        e.CompletionTime <- DateTimeOffset.UtcNow
        e.Completed <- true
        let! e' = Table.merge config table e
        return ()
    }

    member this.GetProcess(pid : string) = async {
        return! Table.read<ProcessRecord> config table pk pid
    }

    member this.GetProcesses () = async {
        return! Table.queryPK<ProcessRecord> config table pk
    }