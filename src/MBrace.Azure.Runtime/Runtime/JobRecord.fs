namespace MBrace.Azure.Runtime

open MBrace.Runtime
open MBrace.Azure
open MBrace.Azure.Runtime.Utilities
open Microsoft.WindowsAzure.Storage.Table
open System
open Nessos.Vagabond
open MBrace.Azure.Runtime.Primitives
open System.Runtime.Serialization
open MBrace.Runtime.Utils

type internal JobKind =
    | TaskRoot = 1
    | Parallel = 2
    | Choice   = 3

type internal JobStatus =
    | Preparing = 0
    | Enqueued  = 1
    | Dequeued  = 2
    | Started   = 3
    | Completed = 4
    | Faulted   = 5

[<AllowNullLiteral>]
type JobRecord(parentTaskId, jobId) = 
    inherit TableEntity(parentTaskId, jobId)
    
    member val Id                 = jobId with get, set
    member val ParentTaskId       = parentTaskId with get, set

    member val Affinity           = null : string with get, set
    member val Kind               = Nullable<int>() with get, set
    member val Index              = Nullable<int>() with get, set
    member val MaxIndex           = Nullable<int>() with get, set

    member val CurrentWorker      = null : string with get, set
    member val Status             = Nullable<int>() with get, set

    member val Size               = Nullable<int64>() with get, set
    member val EnqueueTime        = Nullable<DateTimeOffset>() with get, set
    member val DequeueTime        = Nullable<DateTimeOffset>() with get, set
    member val StartTime          = Nullable<DateTimeOffset>() with get, set
    member val CompletionTime     = Nullable<DateTimeOffset>() with get, set
    member val DeliveryCount      = Nullable<int>() with get, set
    member val Completed          = Nullable<bool>() with get, set
    member val Type               = null : string with get, set
    //member val Dependencies       = null : byte [] with get, set

    new () = new JobRecord(null, null)

    member this.CloneDefault() =
        let p = new JobRecord()
        p.PartitionKey <- this.PartitionKey
        p.RowKey <- this.RowKey
        p.ETag <- this.ETag
        p

    static member FromCloudJob(job : CloudJob) =
        let record = new JobRecord(job.TaskEntry.Id, job.Id)
        
        match job.JobType with
        | TaskRoot -> 
            record.Kind <- nullable(int JobKind.TaskRoot)
        | ParallelChild(i,m) ->
            record.Kind <- nullable(int JobKind.Parallel)
            record.Index <- nullable i
            record.MaxIndex <- nullable m
        | ChoiceChild(i,m) ->
            record.Kind <- nullable(int JobKind.Choice)
            record.Index <- nullable i
            record.MaxIndex <- nullable m
        
        match job.TargetWorker with
        | Some worker -> record.Affinity <- worker.Id
        | _ -> ()

        record.Status <- nullable(int JobStatus.Preparing)
        record.DeliveryCount <- nullable 0
        record.Completed <- nullable false
        record.Type <- PrettyPrinters.Type.prettyPrint job.Type

        record