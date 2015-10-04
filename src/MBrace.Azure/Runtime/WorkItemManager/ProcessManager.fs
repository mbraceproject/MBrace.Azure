namespace MBrace.Azure.Runtime

open System
open System.Runtime.Serialization

open Microsoft.WindowsAzure.Storage.Table

open Nessos.FsPickler

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Components
open MBrace.Azure
open MBrace.Azure.Runtime.Utilities

// TODO
// Add Posting status.
// Use dynamic query when fetching task completion source?

[<AllowNullLiteral>]
type ProcessRecord(taskId) = 
    inherit TableEntity(ProcessRecord.DefaultPartitionKey, taskId)
    member val Id  : string = taskId with get, set

    member val Name : string = null with get, set

    member val Status             = Nullable<int>() with get, set
    member val EnqueuedTime       = Nullable<DateTimeOffset>() with get, set
    member val DequeuedTime       = Nullable<DateTimeOffset>() with get, set
    member val StartTime          = Nullable<DateTimeOffset>() with get, set
    member val CompletionTime     = Nullable<DateTimeOffset>() with get, set
    member val Completed          = Nullable<bool>() with get, set

    member val CancellationTokenSource : byte [] = null with get, set
    member val ResultUri : string = null with get, set
    member val TypeName : string = null with get, set
    member val Type : byte [] = null with get, set
    member val Dependencies : byte [] = null with get, set
    member val AdditionalResources : byte [] = null with get, set

    new () = new ProcessRecord(null)

    member this.CloneDefault() =
        let p = new ProcessRecord()
        p.PartitionKey <- this.PartitionKey
        p.RowKey <- this.RowKey
        p.ETag <- this.ETag
        p

    override this.ToString() = sprintf "task:%A" taskId

    static member DefaultPartitionKey = "task"

    static member CreateNew(taskId : string, info : CloudProcessInfo) =
        let serializer = ProcessConfiguration.Serializer
        let record = new ProcessRecord(taskId)
        record.Completed <- nullable false
        record.StartTime <- nullableDefault
        record.CompletionTime <- nullableDefault
        record.Dependencies <- serializer.Pickle info.Dependencies
        record.EnqueuedTime <- nullable DateTimeOffset.Now
        record.Name <- match info.Name with Some n -> n | None -> null
        record.Status <- nullable(int CloudProcessStatus.Created)
        record.Type <- info.ReturnType.Bytes
        record.TypeName <- info.ReturnTypeName
        record.CancellationTokenSource <- serializer.Pickle info.CancellationTokenSource
        info.AdditionalResources |> Option.iter (fun r -> record.AdditionalResources <- serializer.Pickle r)
        record

    member record.ToCloudProcessInfo() =
        let serializer = ProcessConfiguration.Serializer
        {
            Name = match record.Name with null -> None | name -> Some name
            CancellationTokenSource = serializer.UnPickle record.CancellationTokenSource
            Dependencies = serializer.UnPickle record.Dependencies
            AdditionalResources = match record.AdditionalResources with null -> None | bytes -> Some <| serializer.UnPickle bytes
            ReturnTypeName = record.TypeName
            ReturnType = new Pickle<_>(record.Type)
        }

    static member GetProcessRecord(config : ClusterId, taskId : string) = async {
        return! Table.read<ProcessRecord> config.StorageAccount config.RuntimeTable ProcessRecord.DefaultPartitionKey taskId
    }

[<DataContract; Sealed>]
type internal CloudProcessEntry (config : ClusterId, taskId : string, processInfo : CloudProcessInfo) =
    let [<DataMember(Name = "config")>] config = config
    let [<DataMember(Name = "taskId")>] taskId = taskId
    let [<DataMember(Name = "processInfo")>] processInfo = processInfo

    override this.ToString() = sprintf "task:%A" taskId

    interface ICloudProcessEntry with
        member this.Id: string = taskId

        member this.AwaitResult(): Async<CloudProcessResult> = async {
            let tcs = this :> ICloudProcessEntry
            let! result = tcs.TryGetResult()
            match result with
            | Some r -> return r
            | None ->
                do! Async.Sleep 200
                return! tcs.AwaitResult()
        }
        
        member this.IncrementCompletedWorkItemCount(): Async<unit> = async { return () }
        member this.IncrementFaultedWorkItemCount(): Async<unit> = async { return () }
        member this.IncrementWorkItemCount(): Async<unit> = async { return () }
        
        member this.DeclareStatus(status: CloudProcessStatus): Async<unit> = async {
            let record = new ProcessRecord(taskId)
            record.Status <- nullable(int status)
            record.ETag <- "*"
            let now = nullable DateTimeOffset.Now
            match status with
            | CloudProcessStatus.Created -> 
                record.Completed <- nullable false
                record.EnqueuedTime <- now
            | CloudProcessStatus.WaitingToRun -> 
                record.Completed <- nullable false
                record.DequeuedTime <- now
            | CloudProcessStatus.Running -> 
                record.Completed <- nullable false
                record.StartTime <- now
            | CloudProcessStatus.Faulted
            | CloudProcessStatus.Completed
            | CloudProcessStatus.UserException
            | CloudProcessStatus.Canceled -> 
                record.Completed <- nullable true
                record.CompletionTime <- nullable DateTimeOffset.Now

            | _ -> invalidArg "status" "invalid Cloud process status."

            let! _ = Table.merge config.StorageAccount config.RuntimeTable record
            return ()
        }
        
        member this.GetState(): Async<CloudProcessState> = async {
            let! jobsHandle = Async.StartChild(Table.queryPK<WorkItemRecord> config.StorageAccount config.RuntimeTable taskId)
            let! record = ProcessRecord.GetProcessRecord(config, taskId)
            let! jobs = jobsHandle

            let execTime =
                match record.Completed, record.StartTime, record.CompletionTime with
                | Nullable true, Nullable s, Nullable c ->
                    Finished(s, c - s)
                | Nullable false, Nullable s, _ ->
                    Started(s, DateTimeOffset.Now - s)
                | Nullable false, Null, Null ->
                    NotStarted
                | _ -> 
                    let ex = new InvalidOperationException(sprintf "Invalid record %s" record.Id)
                    ex.Data.Add("record", record)
                    raise ex

            let total = jobs.Count
            let active, completed, faulted =
                jobs
                |> Seq.fold (fun ((a,c,f) as state) workItem ->
                    match enum<WorkItemStatus> workItem.Status.Value with
                    | WorkItemStatus.Preparing 
                    | WorkItemStatus.Enqueued  -> state
                    | WorkItemStatus.Faulted   -> (a, c, f + 1)
                    | WorkItemStatus.Dequeued
                    | WorkItemStatus.Started   -> (a + 1, c, f)
                    | WorkItemStatus.Completed -> (a, c+1, f)
                    | _ as s -> failwith "Invalid WorkItemStatus %A" s) (0, 0, 0)

            return 
                { 
                    Status = enum (record.Status.GetValueOrDefault(-1))
                    Info = (this :> ICloudProcessEntry).Info
                    ExecutionTime = execTime // TODO : dequeued vs running time?
                    MaxActiveWorkItemCount = -1
                    ActiveWorkItemCount = active
                    CompletedWorkItemCount = completed
                    FaultedWorkItemCount = faulted
                    TotalWorkItemCount = total 
                }
        }

        member this.Info: CloudProcessInfo = processInfo
        
        member this.TryGetResult(): Async<CloudProcessResult option> = async {
            let! record = ProcessRecord.GetProcessRecord(config, taskId)
            match record.ResultUri with
            | null -> return None
            | uri ->
                let! result = BlobPersist.ReadPersistedClosure<CloudProcessResult>(config, uri)
                return Some result
        }

        member this.TrySetResult(result: CloudProcessResult, _workerId : IWorkerId): Async<bool> = async {
            let record = new ProcessRecord(taskId)
            let blobUri = guid()
            do! BlobPersist.PersistClosure(config, result, blobUri, allowNewSifts = false)
            record.ResultUri <- blobUri
            record.ETag <- "*"
            let! _record = Table.merge config.StorageAccount config.RuntimeTable record
            return true
        }
        

[<Sealed; DataContract>]
type CloudProcessManager private (config : ClusterId, logger : ISystemLogger) =

    let [<DataMember(Name="config")>] config = config
    let [<DataMember(Name="logger")>] logger = logger

    interface ICloudProcessManager with
        member this.ClearProcess(taskId: string): Async<unit> = async {
            let record = new ProcessRecord(taskId)
            record.ETag <- "*"
            return! Table.delete config.StorageAccount config.RuntimeTable record // TODO : perform full cleanup?
        }
        
        member this.ClearAllProcesses(): Async<unit> = async {
            let! records = Table.queryPK<ProcessRecord> config.StorageAccount config.RuntimeTable ProcessRecord.DefaultPartitionKey
            return! Table.deleteBatch config.StorageAccount config.RuntimeTable records
        }
        
        member this.StartProcess(info: CloudProcessInfo): Async<ICloudProcessEntry> = async {
            let taskId = guid()
            logger.LogInfof "task:%A : creating task" taskId
            let record = ProcessRecord.CreateNew(taskId, info)
            let! _record = Table.insertOrReplace config.StorageAccount config.RuntimeTable record
            let tcs = new CloudProcessEntry(config, taskId, info)
            logger.LogInfof "%A : task created" tcs
            return tcs :> ICloudProcessEntry
        }
        
        member this.GetAllProcesses(): Async<ICloudProcessEntry []> = async {
            let! records = Table.queryPK<ProcessRecord> config.StorageAccount config.RuntimeTable ProcessRecord.DefaultPartitionKey
            return records |> Seq.map(fun r -> new CloudProcessEntry(config, r.Id, r.ToCloudProcessInfo()) :> ICloudProcessEntry) |> Seq.toArray
        }
        
        member this.TryGetProcessById(taskId: string): Async<ICloudProcessEntry option> = async {
            let! record = Table.read<ProcessRecord> config.StorageAccount config.RuntimeTable ProcessRecord.DefaultPartitionKey taskId
            if record = null then return None else return Some(new CloudProcessEntry(config, taskId, record.ToCloudProcessInfo()) :> ICloudProcessEntry)
        }

    static member Create(config : ClusterId, logger) = new CloudProcessManager(config, logger)