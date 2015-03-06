namespace MBrace.Azure.Runtime.Common

open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open MBrace
open MBrace.Azure.Runtime
open System
open System.Diagnostics
open System.Net
open System.Runtime.Serialization
open Nessos.Vagabond
open MBrace.Azure

type ProcessState = 
    | Posted
    | Running
    | Completed
    | KillRequested
    | Killed
    override this.ToString() = 
        match this with
        | Posted -> "Posted"
        | Running -> "Running"
        | Killed -> "Killed"
        | KillRequested -> "Kill requested"
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
    
    member val TotalJobs = 0 with get, set
    member val ActiveJobs = 0 with get, set
    member val CompletedJobs = 0 with get, set
    member val FaultedJobs = 0 with get, set

    member val TypeName : string = typeName with get, set
    member val Type : byte [] = ty with get, set
    member val Dependencies : byte [] = deps with get, set
    member __.UnpickleType () = Configuration.Pickler.UnPickle<Type> __.Type
    member __.UnpickleDependencies () = Configuration.Pickler.UnPickle<AssemblyId list> __.Dependencies
    
    new () = new ProcessRecord(null, null, null, null, null, Unchecked.defaultof<_>, Unchecked.defaultof<_>, false, null, null, null, null)

type ProcessManager private (config : ConfigurationId) = 
    let pk = "ProcessInfo"
    let table = config.RuntimeTable
    
    static member Create(configId : ConfigurationId) = new ProcessManager(configId)

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
    member this.IncreaseTotalJobs(pid : string, ?count) = 
        let count = defaultArg count 1
        Table.transact<ProcessRecord> config table pk pid (fun pr -> pr.TotalJobs <- pr.TotalJobs + count)
        |> Async.Ignore

    member this.AddActiveJob(pid : string) = 
        Table.transact<ProcessRecord> config table pk pid (fun pr -> pr.ActiveJobs <- pr.ActiveJobs + 1)
        |> Async.Ignore

    member this.AddFaultedJob(pid : string) = 
        Table.transact<ProcessRecord> config table pk pid 
            (fun pr -> pr.FaultedJobs <- pr.FaultedJobs + 1
                       pr.TotalJobs <- pr.TotalJobs + 1)
        |> Async.Ignore

    member this.AddCompletedJob(pid : string) = 
        Table.transact<ProcessRecord> config table pk pid 
            (fun pr -> 
                pr.ActiveJobs <- if pr.ActiveJobs = 0 then 0 else pr.ActiveJobs - 1
                pr.CompletedJobs <- pr.CompletedJobs + 1)
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

    member this.SetKillRequested(pid : string) = 
        Table.transact<ProcessRecord> config table pk pid 
          (fun pr -> pr.State <- string ProcessState.KillRequested)
        |> Async.Ignore

    member this.SetCompleted(pid : string) =
        Table.transact<ProcessRecord> config table pk pid 
          (fun pr ->  pr.State <- string ProcessState.Completed
                      pr.CompletionTime <- DateTimeOffset.UtcNow
                      pr.Completed <- true)
        |> Async.Ignore

    member this.GetProcess(pid : string) = Table.read<ProcessRecord> config table pk pid

    member this.GetProcesses () = Table.queryPK<ProcessRecord> config table pk
    
    member this.ClearProcess (pid : string, full, force) = async {
        let! record = this.GetProcess(pid)
        if force = false && not record.Completed then
            failwithf "Cannot clear process %s. Process not completed." pid 
        if record <> Unchecked.defaultof<_> then
            do! Table.delete<ProcessRecord> config table record
        if full then
            let! rks = Table.queryDynamic config table pid
            rks |> Array.iter(fun de -> de.ETag <- "*")
            do! Table.deleteBatch config table rks
            let bc = ConfigurationRegistry.Resolve<StoreClientProvider>(config).BlobClient.GetContainerReference(config.RuntimeContainer)
            let dir = bc.GetDirectoryReference(pid)
            let processBlobs = dir.ListBlobs()
            let refs = processBlobs
                        |> Seq.map (fun b -> dir.GetBlockBlobReference(b.Uri.Segments |> Seq.last))
            do! refs |> Seq.map (fun r -> r.DeleteIfExistsAsync())
                     |> Seq.map Async.AwaitTask
                     |> Async.Parallel
                     |> Async.Ignore
            ()
    }

    member this.ClearAllProcesses (force, full) = async {
        let! ps = this.GetProcesses()
        let xs = ResizeArray<exn>()
        for p in ps do 
            let! result = Async.Catch <| this.ClearProcess(p.Id, force, full)
            match result with
            | Choice2Of2 e -> xs.Add(e)
            | _ -> ()
        if xs.Count > 0 then return raise <| AggregateException(xs)
    }