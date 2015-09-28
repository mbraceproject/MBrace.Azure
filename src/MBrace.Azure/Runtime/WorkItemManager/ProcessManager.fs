namespace MBrace.Azure.Runtime

open System
open System.Runtime.Serialization

open Microsoft.WindowsAzure.Storage.Table

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

module ProcessStatus =
    open MBrace.Runtime

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

    new () = new ProcessRecord(null)

    member this.CloneDefault() =
        let p = new ProcessRecord()
        p.PartitionKey <- this.PartitionKey
        p.RowKey <- this.RowKey
        p.ETag <- this.ETag
        p

    override this.ToString() = sprintf "task:%A" taskId

    static member DefaultPartitionKey = "task"

[<DataContract; Sealed>]
type internal CloudProcessEntry (config : ClusterConfiguration, taskId) =
    static let unpickle (value : byte []) = ProcessConfiguration.Serializer.UnPickle<'T>(value)

    let [<DataMember(Name = "config")>] config = config
    let [<DataMember(Name = "taskId")>] taskId = taskId
    
    let [<IgnoreDataMember>] mutable record = Unchecked.defaultof<Lazy<CacheAtom<ProcessRecord>>>
    let [<IgnoreDataMember>] mutable info = Unchecked.defaultof<Lazy<CloudProcessInfo>>

    [<OnDeserialized>]
    let init (_ : StreamingContext) =
        record <- lazy CacheAtom.Create(Table.read<ProcessRecord> config.StorageAccount config.RuntimeTable ProcessRecord.DefaultPartitionKey taskId, intervalMilliseconds = 200)
        info <-
            lazy
                let record = Async.RunSync(Table.read<ProcessRecord> config.StorageAccount config.RuntimeTable ProcessRecord.DefaultPartitionKey taskId)
                { 
                    Name = if record.Name = null then None else Some record.Name
                    CancellationTokenSource = unpickle record.CancellationTokenSource
                    Dependencies = unpickle record.Dependencies
                    ReturnTypeName = record.TypeName
                    ReturnType = unpickle record.Type
                    AdditionalResources = None // TODO : fix
                }

    do init Unchecked.defaultof<_>

    let getRecord () = record.Value.GetValueAsync()

    override this.ToString() = sprintf "task:%A" taskId

    interface ICloudProcessEntry with
        member this.Id: string = taskId

        member this.AwaitResult(): Async<CloudProcessResult> = 
            async {
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
        
        member this.DeclareStatus(status: CloudProcessStatus): Async<unit> = 
            async {
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
        
        member this.GetState(): Async<CloudProcessState> = 
            async {
                // Fetch all jobRecord for with this taskId as a parent and
                // do all the active/completed, etc calculations.
                // TODO : use WorkItemManager.
                let! recordHandle = Async.StartChild(getRecord())
                let! jobsHandle = Async.StartChild(Table.queryPK<WorkItemRecord> config.StorageAccount config.RuntimeTable taskId)

                let! record = recordHandle
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

                let total = jobs.Length
                let active, completed, faulted =
                    jobs
                    |> Array.fold (fun ((a,c,f) as state) workItem ->
                        match enum<WorkItemStatus> workItem.Status.Value with
                        | WorkItemStatus.Preparing 
                        | WorkItemStatus.Enqueued  -> state
                        | WorkItemStatus.Faulted   -> (a, c, f + 1)
                        | WorkItemStatus.Dequeued
                        | WorkItemStatus.Started   -> (a + 1, c, f)
                        | WorkItemStatus.Completed -> (a, c+1, f)
                        | _ as s -> failwith "Invalid WorkItemStatus %A" s) (0, 0, 0)

                return { Status = enum (record.Status.GetValueOrDefault(-1))
                         Info = (this :> ICloudProcessEntry).Info
                         ExecutionTime = execTime // TODO : dequeued vs running time?
                         MaxActiveWorkItemCount = -1
                         ActiveWorkItemCount = active
                         CompletedWorkItemCount = completed
                         FaultedWorkItemCount = faulted
                         TotalWorkItemCount = total }
            }

        member this.Info: CloudProcessInfo = info.Value
        
        member this.TryGetResult(): Async<CloudProcessResult option> = 
            async {
                let! record = getRecord()
                if record.ResultUri = null then
                    return None
                else
                    let blob = Blob<SiftedClosure<CloudProcessResult>>.FromPath(config, ProcessRecord.DefaultPartitionKey, record.ResultUri)
                    let! sifted = blob.GetValue()
                    let! result = ClosureSifter.UnSiftClosure(config, sifted)
                    return Some result
            }

        member this.TrySetResult(result: CloudProcessResult, _workerId : IWorkerId): Async<bool> = 
            async {
                let record = new ProcessRecord(taskId)
                let blobId = guid()
                let! sifted = ClosureSifter.SiftClosure(config, result, allowNewSifts = false)
                let! _blob = Blob.Create(config, ProcessRecord.DefaultPartitionKey, blobId, fun () -> sifted)
                record.ResultUri <- blobId
                record.ETag <- "*"
                let! _record = Table.merge config.StorageAccount config.RuntimeTable record
                return true
            }
        

[<Sealed; DataContract>]
type CloudProcessManager private (config : ClusterConfiguration, logger : ISystemLogger) =
    static let pickle (value : 'T) = ProcessConfiguration.Serializer.Pickle(value)

    let [<DataMember(Name="config")>] config = config
    let [<DataMember(Name="logger")>] logger = logger

    interface ICloudProcessManager with
        member this.ClearProcess(taskId: string): Async<unit> = 
            async {
                let record = new ProcessRecord(taskId)
                record.ETag <- "*"
                return! Table.delete config.StorageAccount config.RuntimeTable record // TODO : perform full cleanup?
            }
        
        member this.ClearAllProcesses(): Async<unit> = 
            async {
                let! records = Table.queryPK<ProcessRecord> config.StorageAccount config.RuntimeTable ProcessRecord.DefaultPartitionKey
                return! Table.deleteBatch config.StorageAccount config.RuntimeTable records
            }
        
        member this.StartProcess(info: CloudProcessInfo): Async<ICloudProcessEntry> = 
            async {
                let taskId = guid()
                logger.LogInfof "task:%A : creating task" taskId
                let record = new ProcessRecord(taskId)
                record.Completed <- nullable false
                record.StartTime <- nullableDefault
                record.CompletionTime <- nullableDefault
                record.Dependencies <- pickle info.Dependencies
                record.EnqueuedTime <- nullable DateTimeOffset.Now
                record.Name <- match info.Name with Some n -> n | None -> null
                record.Status <- nullable(int CloudProcessStatus.Created)
                record.Type <- pickle info.ReturnType
                record.TypeName <- info.ReturnTypeName
                record.CancellationTokenSource <- pickle info.CancellationTokenSource
                let! _record = Table.insertOrReplace config.StorageAccount config.RuntimeTable record
                let tcs = new CloudProcessEntry(config, taskId)
                logger.LogInfof "%A : task created" tcs
                return tcs :> ICloudProcessEntry
            }
        
        member this.GetAllProcesses(): Async<ICloudProcessEntry []> = 
            async {
                let! records = Table.queryPK<ProcessRecord> config.StorageAccount config.RuntimeTable ProcessRecord.DefaultPartitionKey
                return records |> Array.map(fun r -> new CloudProcessEntry(config, r.Id) :> ICloudProcessEntry)
            }
        
        member this.TryGetProcessById(taskId: string): Async<ICloudProcessEntry option> = 
            async {
                let! record = Table.read<ProcessRecord> config.StorageAccount config.RuntimeTable ProcessRecord.DefaultPartitionKey taskId
                if record = null then return None else return Some(new CloudProcessEntry(config, taskId) :> ICloudProcessEntry)
            }

    static member Create(config : ClusterConfiguration, logger) = new CloudProcessManager(config, logger)