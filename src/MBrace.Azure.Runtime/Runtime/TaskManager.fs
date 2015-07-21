namespace MBrace.Azure.Runtime

open MBrace.Runtime
open MBrace.Azure
open MBrace.Azure.Runtime.Utilities
open Microsoft.WindowsAzure.Storage.Table
open System
open Nessos.Vagabond
open MBrace.Azure.Runtime.Primitives

// TODO
// Add Posting status.
// Use dynamic query when fetching task completion source?

module TaskStatus =
    open MBrace.Runtime
    let ofInt status : CloudTaskStatus =
        match status with
        | 0 -> Posted
        | 1 -> Dequeued
        | 2 -> Running
        | 3 -> Faulted
        | 4 -> Completed
        | 5 -> UserException
        | 6 -> Canceled
        | _ -> failwithf "Failed to convert %O to CloudTaskStatus." status

    let toInt (status : CloudTaskStatus) =
        match status with
        | Posted -> 0
        | Dequeued -> 1
        | Running -> 2
        | Faulted -> 3
        | Completed -> 4
        | UserException -> 5
        | Canceled -> 6

type TaskRecord(pk, taskId) = 
    inherit TableEntity(pk, taskId)
    member val Id  : string = taskId with get, set

    member val Name : string = null with get, set

    member val Status             = Nullable<int>() with get, set
    member val InitializationTime = Nullable<DateTimeOffset>() with get, set
    member val CompletionTime     = Nullable<DateTimeOffset>() with get, set
    member val Completed          = Nullable<bool>() with get, set

    member val CancellationPartitionKey : string = null with get, set
    member val CancellationRowKey : string = null with get, set
    
    member val TotalJobs     = Nullable<int>() with get, set
    member val ActiveJobs    = Nullable<int>() with get, set
    member val CompletedJobs = Nullable<int>() with get, set
    member val FaultedJobs   = Nullable<int>() with get, set

    member val TypeName : string = null with get, set
    member val Type : byte [] = null with get, set
    member val Dependencies : byte [] = null with get, set
    member __.UnpickleType () = Configuration.Pickler.UnPickle<Type> __.Type
    member __.UnpickleDependencies () = Configuration.Pickler.UnPickle<AssemblyId []> __.Dependencies
    
    new () = new TaskRecord(null, null)

    member this.CloneDefault() =
        let p = new TaskRecord()
        p.PartitionKey <- this.PartitionKey
        p.RowKey <- this.RowKey
        p.ETag <- this.ETag
        p

type TaskCompletionSource (config : ConfigurationId, partitionKey, taskId) =
    interface ICloudTaskCompletionSource with
        member this.Id: string = taskId

        member this.AwaitResult(): Async<TaskResult> = 
            failwith "Not implemented yet"
        
        member this.DeclareCompletedJob(): Async<unit> = 
            failwith "Not implemented yet"
        
        member this.DeclareFaultedJob(): Async<unit> = 
            failwith "Not implemented yet"
        
        member this.DeclareStatus(status: CloudTaskStatus): Async<unit> = 
            failwith "Not implemented yet"
        
        member this.GetState(): Async<CloudTaskState> = 
            failwith "Not implemented yet"
        
        member this.IncrementJobCount(): Async<unit> = 
            failwith "Not implemented yet"
        
        member this.Info: CloudTaskInfo = 
            failwith "Not implemented yet"
        
        member this.TryGetResult(): Async<TaskResult option> = 
            failwith "Not implemented yet"
        
        member this.TrySetResult(result: TaskResult): Async<bool> = 
            failwith "Not implemented yet"
        

[<AutoSerializable(true)>]
type TaskManager private (config : ConfigurationId) =
    let partitionKey = "task"

    let table = config.RuntimeTable
    let pickle (value : 'T) = Configuration.Pickler.Pickle(value)
    let unpickle (value : byte []) = Configuration.Pickler.UnPickle<'T>(value)

    interface ICloudTaskManager with
        member this.Clear(taskId: string): Async<unit> = 
            async {
                let record = new TaskRecord(partitionKey, taskId)
                return! Table.delete config table record // TODO : perform full cleanup?
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
                let tcs = new TaskCompletionSource(config, partitionKey, guid())
                let cts = info.CancellationTokenSource :?> DistributedCancellationTokenSource
                let _elevated = cts.ElevateCancellationToken()
                let record = new TaskRecord(partitionKey, guid())
                record.ActiveJobs <- nullable 0
                record.CancellationPartitionKey <- cts.PartitionKey
                record.CancellationRowKey <- cts.RowKey.Value
                record.Completed <- nullable false
                record.CompletedJobs <- nullable 0
                record.CompletionTime <- nullableDefault
                record.Dependencies <- pickle info.Dependencies
                record.FaultedJobs <- nullable 0
                record.InitializationTime <- nullable DateTimeOffset.Now
                record.Name <- match info.Name with Some n -> n | None -> null
                record.Status <- nullable(TaskStatus.toInt(CloudTaskStatus.Posted))
                record.TotalJobs <- nullable 0
                record.Type <- info.ReturnType.Bytes
                record.TypeName <- info.ReturnTypeName
                return tcs :> _
            }
        
        member this.GetAllTasks(): Async<ICloudTaskCompletionSource []> = 
            async {
                let! records = Table.queryPK<TaskRecord> config table partitionKey
                return records |> Array.map(fun r -> new TaskCompletionSource(config, partitionKey, r.Id) :> ICloudTaskCompletionSource)
            }
        
        member this.TryGetTaskById(taskId: string): Async<ICloudTaskCompletionSource option> = 
            async {
                let! record = Table.read config table partitionKey taskId
                if record = null then return None else return Some(new TaskCompletionSource(config, partitionKey, taskId) :> ICloudTaskCompletionSource)
            }

    static member Create(config : ConfigurationId) = new TaskManager(config)