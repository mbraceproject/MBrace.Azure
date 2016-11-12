namespace MBrace.Azure.Runtime

open System
open System.Runtime.Serialization

open Microsoft.WindowsAzure.Storage.Table

open MBrace.FsPickler

open MBrace.Core
open MBrace.Runtime
open MBrace.Azure
open MBrace.Azure.Runtime.Utilities

// TODO
// Add Posting status.
// Use dynamic query when fetching task completion source?

[<AllowNullLiteral>]
type CloudProcessRecord(taskId) = 
    inherit TableEntity(CloudProcessRecord.DefaultPartitionKey, taskId)
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

    new () = new CloudProcessRecord(null)

    member this.CloneDefault() =
        let p = new CloudProcessRecord()
        p.PartitionKey <- this.PartitionKey
        p.RowKey <- this.RowKey
        p.ETag <- this.ETag
        p

    override this.ToString() = sprintf "task:%A" taskId

    static member DefaultPartitionKey = "task"

    static member CreateNew(taskId : string, info : CloudProcessInfo) =
        let serializer = ProcessConfiguration.BinarySerializer
        let record = new CloudProcessRecord(taskId)
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
        let serializer = ProcessConfiguration.BinarySerializer
        {
            Name = match record.Name with null -> None | name -> Some name
            CancellationTokenSource = serializer.UnPickle record.CancellationTokenSource
            Dependencies = serializer.UnPickle record.Dependencies
            AdditionalResources = match record.AdditionalResources with null -> None | bytes -> Some <| serializer.UnPickle bytes
            ReturnTypeName = record.TypeName
            ReturnType = new Pickle<_>(record.Type)
        }

    static member GetProcessRecord(clusterId : ClusterId, taskId : string) = async {
        return! Table.read<CloudProcessRecord> clusterId.StorageAccount clusterId.RuntimeTable CloudProcessRecord.DefaultPartitionKey taskId
    }

[<DataContract; Sealed>]
type internal CloudProcessEntry (clusterId : ClusterId, processId : string, processInfo : CloudProcessInfo) =
    let [<DataMember(Name = "ClusterId")>] clusterId = clusterId
    let [<DataMember(Name = "ProcessId")>] taskId = processId
    let [<DataMember(Name = "ProcessInfo")>] processInfo = processInfo

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

        member this.WaitAsync(): Async<unit> = async {
            let! record = CloudProcessRecord.GetProcessRecord(clusterId, taskId)
            // result uri has been populated, hence computation has completed
            if record.ResultUri <> null then return ()
            else
                do! Async.Sleep 200
                return! (this :> ICloudProcessEntry).WaitAsync()
        }
        
        member this.IncrementCompletedWorkItemCount(): Async<unit> = async { return () }
        member this.IncrementFaultedWorkItemCount(): Async<unit> = async { return () }
        member this.IncrementWorkItemCount(): Async<unit> = async { return () }
        
        member this.DeclareStatus(status: CloudProcessStatus): Async<unit> = async {
            let record = new CloudProcessRecord(taskId)
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

            let! _ = Table.merge clusterId.StorageAccount clusterId.RuntimeTable record
            return ()
        }
        
        member this.GetState(): Async<CloudProcessState> = async {
            let! jobsHandle = Async.StartChild(Table.queryPK<WorkItemRecord> clusterId.StorageAccount clusterId.RuntimeTable taskId)
            let! record = CloudProcessRecord.GetProcessRecord(clusterId, taskId)
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
                    | WorkItemStatus.Completed -> (a, c + 1, f)
                    | _ as s -> failwithf "Invalid WorkItemStatus %A" s) (0, 0, 0)

            return 
                { 
                    Status = enum (record.Status.GetValueOrDefault(-1))
                    Info = (this :> ICloudProcessEntry).Info
                    ExecutionTime = execTime // TODO : dequeued vs running time?
                    ActiveWorkItemCount = active
                    CompletedWorkItemCount = completed
                    FaultedWorkItemCount = faulted
                    TotalWorkItemCount = total 
                }
        }

        member this.Info: CloudProcessInfo = processInfo
        
        member this.TryGetResult(): Async<CloudProcessResult option> = async {
            let! record = CloudProcessRecord.GetProcessRecord(clusterId, taskId)
            match record.ResultUri with
            | null -> return None
            | uri ->
                let! result = BlobPersist.ReadPersistedClosure<CloudProcessResult>(clusterId, uri)
                return Some result
        }

        member this.TrySetResult(result: CloudProcessResult, _workerId : IWorkerId): Async<bool> = async {
            let record = new CloudProcessRecord(taskId)
            let blobUri = guid()
            do! BlobPersist.PersistClosure(clusterId, result, blobUri, allowNewSifts = false)
            record.ResultUri <- blobUri
            record.ETag <- "*"
            let! _record = Table.merge clusterId.StorageAccount clusterId.RuntimeTable record
            return true
        }