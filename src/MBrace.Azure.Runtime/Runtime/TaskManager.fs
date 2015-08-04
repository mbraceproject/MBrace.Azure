namespace MBrace.Azure.Runtime

open MBrace.Runtime
open MBrace.Azure
open MBrace.Azure.Runtime.Utilities
open Microsoft.WindowsAzure.Storage.Table
open System
open Nessos.Vagabond
open System.Runtime.Serialization
open MBrace.Runtime.Utils
open Nessos.FsPickler

// TODO
// Add Posting status.
// Use dynamic query when fetching task completion source?

module TaskStatus =
    open MBrace.Runtime
    let ofInt status : CloudTaskStatus =
        match status with
        | 0 -> raise <| NotSupportedException(string status)
        | 1 -> Posted
        | 2 -> Dequeued
        | 3 -> Running
        | 4 -> Faulted
        | 5 -> Completed
        | 6 -> UserException
        | 7 -> Canceled
        | _ -> failwithf "Failed to convert %O to CloudTaskStatus." status

    let toInt (status : CloudTaskStatus) =
        match status with
        | Posted        -> 1
        | Dequeued      -> 2
        | Running       -> 3
        | Faulted       -> 4
        | Completed     -> 5
        | UserException -> 6
        | Canceled      -> 7

[<AllowNullLiteral>]
type TaskRecord(taskId) = 
    inherit TableEntity(TaskRecord.DefaultPartitionKey, taskId)
    member val Id  : string = taskId with get, set

    member val Name : string = null with get, set

    member val Status             = Nullable<int>() with get, set
    member val EnqueuedTime       = Nullable<DateTimeOffset>() with get, set
    member val DequeuedTime       = Nullable<DateTimeOffset>() with get, set
    member val StartTime          = Nullable<DateTimeOffset>() with get, set
    member val CompletionTime     = Nullable<DateTimeOffset>() with get, set
    member val Completed          = Nullable<bool>() with get, set

    member val TotalJobs     = Nullable<int>() with get, set
    member val ActiveJobs    = Nullable<int>() with get, set
    member val CompletedJobs = Nullable<int>() with get, set
    member val FaultedJobs   = Nullable<int>() with get, set

    member val CancellationTokenSource : byte [] = null with get, set
    member val ResultUri : string = null with get, set
    member val TypeName : string = null with get, set
    member val Type : byte [] = null with get, set
    member val Dependencies : byte [] = null with get, set

    new () = new TaskRecord(null)

    member this.CloneDefault() =
        let p = new TaskRecord()
        p.PartitionKey <- this.PartitionKey
        p.RowKey <- this.RowKey
        p.ETag <- this.ETag
        p

    override this.ToString() = sprintf "task:%A" taskId

    static member DefaultPartitionKey = "task"

[<DataContract; Sealed>]
type internal TaskCompletionSource (config : ConfigurationId, taskId) =
    static let unpickle (value : byte []) = Config.Pickler.UnPickle<'T>(value)

    let [<DataMember(Name = "config")>] config = config
    let [<DataMember(Name = "taskId")>] taskId = taskId
    
    let [<IgnoreDataMember>] mutable record = Unchecked.defaultof<Lazy<CacheAtom<TaskRecord>>>
    let [<IgnoreDataMember>] mutable info = Unchecked.defaultof<Lazy<CloudTaskInfo>>

    [<OnDeserialized>]
    let init (_ : StreamingContext) =
        record <- lazy CacheAtom.Create(Table.read<TaskRecord> config config.RuntimeTable TaskRecord.DefaultPartitionKey taskId, intervalMilliseconds = 200)
        info <-
            lazy
                let record = Async.RunSynchronously(Table.read<TaskRecord> config config.RuntimeTable TaskRecord.DefaultPartitionKey taskId)
                { 
                    Name = if record.Name = null then None else Some record.Name
                    CancellationTokenSource = unpickle record.CancellationTokenSource
                    Dependencies = unpickle record.Dependencies
                    ReturnTypeName = record.TypeName
                    ReturnType = unpickle record.Type
                }
    do init Unchecked.defaultof<_>

    let getRecord () = record.Value.GetValueAsync()

    override this.ToString() = sprintf "task:%A" taskId

    interface ICloudTaskCompletionSource with
        member this.Id: string = taskId

        member this.AwaitResult(): Async<TaskResult> = 
            async {
                let tcs = this :> ICloudTaskCompletionSource
                let! result = tcs.TryGetResult()
                match result with
                | Some r -> return r
                | None ->
                    do! Async.Sleep 200
                    return! tcs.AwaitResult()
            }
        
        member this.DeclareCompletedJob(): Async<unit> = async { return () }
        member this.DeclareFaultedJob(): Async<unit> = async { return () }
        member this.IncrementJobCount(): Async<unit> = async { return () }
        
        member this.DeclareStatus(status: CloudTaskStatus): Async<unit> = 
            async {
                let record = new TaskRecord(taskId)
                record.Status <- nullable(TaskStatus.toInt status)
                record.ETag <- "*"
                let now = nullable DateTimeOffset.Now
                match status with
                | Posted -> 
                    record.Completed <- nullable false
                    record.EnqueuedTime <- now
                | Dequeued -> 
                    record.Completed <- nullable false
                    record.DequeuedTime <- now
                | Running -> 
                    record.Completed <- nullable false
                    record.StartTime <- now
                | Faulted
                | Completed
                | UserException
                | Canceled -> 
                    record.Completed <- nullable true
                    record.CompletionTime <- nullable DateTimeOffset.Now
                let! _ = Table.merge config config.RuntimeTable record
                return ()
            }
        
        member this.GetState(): Async<CloudTaskState> = 
            async {
                let! record = getRecord()
                let execTime =
                    match record.Completed, record.StartTime, record.CompletionTime with
                    | Nullable true, Nullable s, Nullable c ->
                        Finished(s.DateTime, c-s, c.DateTime)
                    | Nullable false, Nullable s, _ ->
                        Started(s.DateTime, DateTimeOffset.Now - s)
                    | Nullable false, Null, Null ->
                        NotStarted
                    | _ -> 
                        let ex = RuntimeException(sprintf "Invalid record %s" record.Id)
                        ex.Data.Add("record", record)
                        raise ex

                return { Status = TaskStatus.ofInt(record.Status.GetValueOrDefault(-1))
                         Info = (this :> ICloudTaskCompletionSource).Info
                         ExecutionTime = execTime // TODO : dequeued vs running time?
                         MaxActiveJobCount = -1
                         ActiveJobCount = record.ActiveJobs.GetValueOrDefault(-1)
                         CompletedJobCount = record.CompletedJobs.GetValueOrDefault(-1)
                         FaultedJobCount = record.FaultedJobs.GetValueOrDefault(-1)
                         TotalJobCount = record.TotalJobs.GetValueOrDefault(-1) }
            }

        member this.Info: CloudTaskInfo = info.Value
        
        member this.TryGetResult(): Async<TaskResult option> = 
            async {
                let! record = getRecord()
                if record.ResultUri = null then
                    return None
                else
                    let blob = Blob<TaskResult>.FromPath(config, TaskRecord.DefaultPartitionKey, record.ResultUri)
                    let! result = blob.GetValue()
                    return Some result
            }

        member this.TrySetResult(result: TaskResult): Async<bool> = 
            async {
                let record = new TaskRecord(taskId)
                let blobId = guid()
                let! _blob = Blob.Create(config, TaskRecord.DefaultPartitionKey, blobId, fun () -> result)
                record.ResultUri <- blobId
                record.ETag <- "*"
                let! _record = Table.merge config config.RuntimeTable record
                return true
            }
        

[<Sealed; DataContract>]
type TaskManager private (config : ConfigurationId, logger : ISystemLogger) =
    static let pickle (value : 'T) = Config.Pickler.Pickle(value)

    let [<DataMember(Name="config")>] config = config
    let [<DataMember(Name="logger")>] logger = logger

    interface ICloudTaskManager with
        member this.Clear(taskId: string): Async<unit> = 
            async {
                let record = new TaskRecord(taskId)
                return! Table.delete config config.RuntimeTable record // TODO : perform full cleanup?
            }
        
        member this.ClearAllTasks(): Async<unit> = 
            async {
                let taskManager = this :> ICloudTaskManager
                let! tasks = taskManager.GetAllTasks()
                return! tasks
                        |> Seq.map (fun t -> taskManager.Clear(t.Id))
                        |> Async.Parallel
                        |> Async.Ignore
            }
        
        member this.CreateTask(info: CloudTaskInfo): Async<ICloudTaskCompletionSource> = 
            async {
                let taskId = guid()
                logger.LogInfof "task:%A : creating task" taskId
                let record = new TaskRecord(taskId)
                record.ActiveJobs <- nullable 0
                record.Completed <- nullable false
                record.CompletedJobs <- nullable 0
                record.StartTime <- nullableDefault
                record.CompletionTime <- nullableDefault
                record.Dependencies <- pickle info.Dependencies
                record.FaultedJobs <- nullable 0
                record.EnqueuedTime <- nullable DateTimeOffset.Now
                record.Name <- match info.Name with Some n -> n | None -> null
                record.Status <- nullable(TaskStatus.toInt(CloudTaskStatus.Posted))
                record.TotalJobs <- nullable 0
                record.Type <- pickle info.ReturnType
                record.TypeName <- info.ReturnTypeName
                record.CancellationTokenSource <- pickle info.CancellationTokenSource
                let! _record = Table.insertOrReplace config config.RuntimeTable record
                let tcs = new TaskCompletionSource(config, taskId)
                logger.LogInfof "%A : task created" tcs
                return tcs :> ICloudTaskCompletionSource
            }
        
        member this.GetAllTasks(): Async<ICloudTaskCompletionSource []> = 
            async {
                let! records = Table.queryPK<TaskRecord> config config.RuntimeTable TaskRecord.DefaultPartitionKey
                return records |> Array.map(fun r -> new TaskCompletionSource(config, r.Id) :> ICloudTaskCompletionSource)
            }
        
        member this.TryGetTaskById(taskId: string): Async<ICloudTaskCompletionSource option> = 
            async {
                let! record = Table.read<TaskRecord> config config.RuntimeTable TaskRecord.DefaultPartitionKey taskId
                if record = null then return None else return Some(new TaskCompletionSource(config, taskId) :> ICloudTaskCompletionSource)
            }

    static member Create(config : ConfigurationId, logger) = new TaskManager(config, logger)