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
    | Posted
    | Running
    | Completed
    | Cancelling
    | Killed
    override this.ToString() = 
        match this with
        | Posted -> "Posted"
        | Running -> "Running"
        | Killed -> "Killed"
        | Cancelling -> "Cancelling"
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
    
    member val TotalTasks = 0 with get, set
    member val ActiveTasks = 0 with get, set
    member val CompletedTasks = 0 with get, set
    member val FaultedTasks = 0 with get, set

    member val TypeName : string = typeName with get, set
    member val Type : byte [] = ty with get, set
    member val Dependencies : byte [] = deps with get, set
    member __.UnpickleType () = Configuration.Pickler.UnPickle<Type> __.Type
    member __.UnpickleDependencies () = Configuration.Pickler.UnPickle<AssemblyId list> __.Dependencies
    
    new () = new ProcessRecord(null, null, null, null, null, Unchecked.defaultof<_>, Unchecked.defaultof<_>, false, null, null, null, null)

type ProcessMonitor private (config, table : string) = 
    let pk = "process"
    
    static member Create(config : Configuration) = new ProcessMonitor(config.ConfigurationId, config.DefaultTableOrContainer)

    member this.CreateRecord(pid : string, name, ty : Type, deps : AssemblyId list, ctsUri, resultUri) = async { 
        let now = DateTimeOffset.UtcNow
        let pickledTy = Configuration.Pickler.Pickle(ty)
        let deps = Configuration.Pickler.Pickle(deps)
        let tyName = Runtime.Utils.PrettyPrinters.Type.prettyPrint ty
        let e = new ProcessRecord(pk, pid, name, ctsUri, string ProcessState.Posted, now, now, false, resultUri, pickledTy, tyName, deps)
        do! Table.insertOrReplace<ProcessRecord> config table e
        return e
    }

    // TODO : These methods cannot be used atomically
    member this.IncreaseTotalTasks(pid : string, ?count) = 
        let count = defaultArg count 1
        Table.transact<ProcessRecord> config table pk pid (fun pr -> pr.TotalTasks <- pr.TotalTasks + count)
        |> Async.Ignore

    member this.AddActiveTask(pid : string) = 
        Table.transact<ProcessRecord> config table pk pid (fun pr -> pr.ActiveTasks <- pr.ActiveTasks + 1)
        |> Async.Ignore

    member this.AddFaultedTask(pid : string) = 
        Table.transact<ProcessRecord> config table pk pid 
            (fun pr -> pr.FaultedTasks <- pr.FaultedTasks + 1
                       pr.TotalTasks <- pr.TotalTasks + 1)
        |> Async.Ignore

    member this.AddCompletedTask(pid : string) = 
        Table.transact<ProcessRecord> config table pk pid 
            (fun pr -> 
                pr.ActiveTasks <- if pr.ActiveTasks = 0 then 0 else pr.ActiveTasks - 1
                pr.CompletedTasks <- pr.CompletedTasks + 1)
        |> Async.Ignore

    member this.SetRunning(pid : string) = 
        Table.transact<ProcessRecord> config table pk pid 
           (fun pr -> 
               if pr.State = string ProcessState.Posted then
                   pr.State <- string ProcessState.Running
                   pr.InitializationTime <- DateTimeOffset.UtcNow)
        |> Async.Ignore

    member this.SetKilled(pid : string) = 
        Table.transact<ProcessRecord> config table pk pid 
          (fun pr -> 
              pr.State <- string ProcessState.Killed
              pr.CompletionTime <- DateTimeOffset.UtcNow
              pr.Completed <- true)
        |> Async.Ignore

    member this.SetCancelling(pid : string) = 
        Table.transact<ProcessRecord> config table pk pid 
          (fun pr -> pr.State <- string ProcessState.Cancelling)
        |> Async.Ignore

    member this.SetCompleted(pid : string) =
        Table.transact<ProcessRecord> config table pk pid 
          (fun pr ->  pr.State <- string ProcessState.Completed
                      pr.CompletionTime <- DateTimeOffset.UtcNow
                      pr.Completed <- true)
        |> Async.Ignore

    member this.GetProcess(pid : string) = Table.read<ProcessRecord> config table pk pid

    member this.GetProcesses () = Table.queryPK<ProcessRecord> config table pk

    member this.ClearProcessStorage (pid : string) = async {
        let container = Storage.processIdToStorageId pid
        let provider = ConfigurationRegistry.Resolve<ClientProvider>(config)
        let tableRef = provider.TableClient.GetTableReference(container)
        do! tableRef.DeleteIfExistsAsync()
        let containerRef = provider.BlobClient.GetContainerReference(container)
        do! containerRef.DeleteIfExistsAsync()
        return ()
    }