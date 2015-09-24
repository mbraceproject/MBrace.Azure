namespace MBrace.Azure.Runtime

open MBrace.Runtime
open MBrace.Azure.Runtime.Utilities
open Microsoft.WindowsAzure.Storage.Table
open System
open MBrace.Runtime.Utils

type internal WorkItemKind =
    | ProcessRoot = 1
    | Parallel = 2
    | Choice   = 3

type internal WorkItemStatus =
    | Preparing = 0
    | Enqueued  = 1
    | Dequeued  = 2
    | Started   = 3
    | Completed = 4
    | Faulted   = 5

type internal FaultInfo =
    | NoFault                       = 0
    | FaultDeclaredByWorker         = 1
    | WorkerDeathWhileProcessingWorkItem = 2
    | IsTargetedWorkItemOfDeadWorker     = 3

[<AllowNullLiteral>]
type WorkItemRecord(parentTaskId : string, jobId : string) = 
    inherit TableEntity(parentTaskId, jobId)
    
    member val Id                 = jobId with get
    member val ParentTaskId       = parentTaskId with get

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
    member val RenewLockTime      = Nullable<DateTimeOffset>() with get, set
    member val Completed          = Nullable<bool>() with get, set
    member val Type               = null : string with get, set
    member val LastException      = null : byte [] with get, set
    member val FaultInfo          = Nullable<int>() with get, set

    new () = new WorkItemRecord(null, null)

    override this.ToString() = sprintf "workItem:%A" jobId

    member this.CloneDefault() =
        let p = new WorkItemRecord(this.PartitionKey, this.RowKey)
        p.ETag <- this.ETag
        p

    static member FromCloudWorkItem(workItem : CloudWorkItem) =
        let record = new WorkItemRecord(workItem.Process.Id, fromGuid workItem.Id)
        
        match workItem.WorkItemType with
        | ProcessRoot -> 
            record.Kind <- nullable(int WorkItemKind.ProcessRoot)
        | ParallelChild(i,m) ->
            record.Kind <- nullable(int WorkItemKind.Parallel)
            record.Index <- nullable i
            record.MaxIndex <- nullable m
        | ChoiceChild(i,m) ->
            record.Kind <- nullable(int WorkItemKind.Choice)
            record.Index <- nullable i
            record.MaxIndex <- nullable m
        
        match workItem.TargetWorker with
        | Some worker -> record.Affinity <- worker.Id
        | _ -> ()

        record.Status <- nullable(int WorkItemStatus.Preparing)
        record.DeliveryCount <- nullable 0
        record.Completed <- nullable false
        record.Type <- PrettyPrinters.Type.prettyPrintUntyped workItem.Type
        record.FaultInfo <- nullable(int FaultInfo.NoFault)
        record