namespace MBrace.Azure.Runtime

open System
open System.IO
open System.Threading
open System.Runtime.Serialization

open Microsoft.FSharp.Control
open Microsoft.ServiceBus.Messaging

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Azure.Runtime.Utilities

/// WorkItem info stored in BrokeredMessage.
type internal WorkItemLeaseTokenInfo =
    {
        DeliveryCount : int
        WorkItemId : Guid
        BlobUri : string
        MessageLockId : Guid
        ProcessId : string
        BatchIndex : int option
        TargetWorker : string option
        DequeueTime : DateTimeOffset
    }
with
    override this.ToString() = sprintf "leaseinfo:%A" this.WorkItemId

    static member FromReceivedMessage(message : BrokeredMessage) =
        let topicDeliveryCount = message.TryGetProperty<int> ServiceBusSettings.TopicDeliveryCount
        let deliveryCount = message.DeliveryCount + (defaultArg topicDeliveryCount 0)
        {
            WorkItemId = message.GetProperty<Guid> ServiceBusSettings.WorkItemIdProperty
            BlobUri = message.GetBody<string>()
            MessageLockId = message.LockToken
            ProcessId = message.GetProperty<string> ServiceBusSettings.ParentTaskIdProperty
            BatchIndex = message.TryGetProperty<int> ServiceBusSettings.BatchIndexProperty
            TargetWorker = message.TryGetProperty<string> ServiceBusSettings.AffinityProperty
            DeliveryCount = deliveryCount
            DequeueTime = DateTimeOffset.Now
        }

type internal LeaseAction =
    | Complete
    | Abandon

/// Periodically renews lock for supplied work item, releases lock if specified as completed.
[<Sealed; AutoSerializable(false)>]
type internal WorkItemLeaseMonitor private (clusterId : ClusterId, message : BrokeredMessage, info : WorkItemLeaseTokenInfo, logger : ISystemLogger) =

    let rec renewLoop (inbox : MailboxProcessor<LeaseAction>) = async {
        let! msg = inbox.TryReceive(timeout = 10)
        match msg with
        | None ->
            let! renewResult = Async.Catch <| async {
                do! message.RenewLockAsync()
                let record = new WorkItemRecord(info.ProcessId, fromGuid info.WorkItemId)
                record.RenewLockTime <- nullable DateTimeOffset.Now
                record.ETag <- "*"
                let! _updated = Table.merge clusterId.StorageAccount clusterId.RuntimeTable record
                return ()
            }

            match renewResult with
            | Choice1Of2 () ->
                logger.Logf LogLevel.Debug "%A : lock renewed" info
                do! Async.Sleep ServiceBusSettings.RenewLockInverval
                return! renewLoop inbox

            | Choice2Of2 (:? MessageLockLostException) -> 
                logger.Logf LogLevel.Warning "%A : lock lost" info
                message.Dispose()

            | e ->
                logger.LogErrorf "%A : lock renew failed with %A" info e
                do! Async.Sleep ServiceBusSettings.RenewLockInverval
                return! renewLoop inbox

        | Some Complete ->
            match info.TargetWorker with
            | None -> 
                let queue = clusterId.ServiceBusAccount.CreateQueueClient(clusterId.WorkItemQueue, ReceiveMode.PeekLock)
                return! queue.CompleteAsync(info.MessageLockId)
            | Some affinity -> 
                let subscription = clusterId.ServiceBusAccount.CreateSubscriptionClient(clusterId.WorkItemTopic, affinity)
                return! subscription.CompleteAsync(info.MessageLockId)

            logger.Logf LogLevel.Info "%A : completed" info
            message.Dispose()

        | Some Abandon ->
            match info.TargetWorker with
            | None -> 
                let queue = clusterId.ServiceBusAccount.CreateQueueClient(clusterId.WorkItemQueue, ReceiveMode.PeekLock)
                return! queue.AbandonAsync(info.MessageLockId)
            | Some affinity -> 
                let subscription = clusterId.ServiceBusAccount.CreateSubscriptionClient(clusterId.WorkItemTopic, affinity)
                return! subscription.AbandonAsync(info.MessageLockId)

            logger.Logf LogLevel.Info "%A : abandoned" info
            message.Dispose()
    }

    let cts = new CancellationTokenSource()
    let mbox = MailboxProcessor.Start(renewLoop, cts.Token)

    member __.CompleteWith(action) = mbox.Post action

    interface IDisposable with member __.Dispose() = cts.Cancel()

    static member Start(id : ClusterId, message : BrokeredMessage, info : WorkItemLeaseTokenInfo, logger : ISystemLogger) =
        new WorkItemLeaseMonitor(id, message, info, logger)

/// Implements ICloudWorkItemLeaseToken
type internal WorkItemLeaseToken =
    {
        ClusterId       : ClusterId
        CompleteAction  : MarshaledAction<LeaseAction> // ensures that LeaseMonitor is serializable across AppDomains
        WorkItemType    : CloudWorkItemType
        WorkItemSize    : int64
        TypeName        : string
        FaultInfo       : CloudWorkItemFaultInfo
        LeaseInfo       : WorkItemLeaseTokenInfo
        ProcessInfo     : CloudProcessInfo
    }
with
    interface ICloudWorkItemLeaseToken with
        member this.DeclareCompleted() : Async<unit> = async {
            this.CompleteAction.Invoke Complete
            this.CompleteAction.Dispose() // disconnect marshaled object
            let record = new WorkItemRecord(this.LeaseInfo.ProcessId, fromGuid this.LeaseInfo.WorkItemId)
            record.Status <- nullable(int WorkItemStatus.Completed)
            record.CompletionTime <- nullable(DateTimeOffset.Now)
            record.Completed <- nullable true
            record.ETag <- "*" 
            let! _record = Table.merge this.ClusterId.StorageAccount this.ClusterId.RuntimeTable record
            return ()
        }
        
        member this.DeclareFaulted(edi : ExceptionDispatchInfo) : Async<unit> = async { 
            this.CompleteAction.Invoke Abandon
            this.CompleteAction.Dispose() // disconnect marshaled object
            let record = new WorkItemRecord(this.LeaseInfo.ProcessId, fromGuid this.LeaseInfo.WorkItemId)
            record.Status <- nullable(int WorkItemStatus.Faulted)
            record.Completed <- nullable false
            record.CompletionTime <- nullableDefault
            // there exists a remote possibility that fault exceptions might be of arbitrary size
            // should probably persist payload to blob as done with results
            record.LastException <- ProcessConfiguration.JsonSerializer.PickleToString edi
            record.FaultInfo <- nullable(int FaultInfo.FaultDeclaredByWorker)
            record.ETag <- "*"
            let! _record = Table.merge this.ClusterId.StorageAccount this.ClusterId.RuntimeTable record
            return () 
        }
        
        member this.FaultInfo : CloudWorkItemFaultInfo = this.FaultInfo
        
        member this.GetWorkItem() : Async<CloudWorkItem> = async { 
            let! payload = BlobPersist.ReadPersistedClosure<MessagePayload>(this.ClusterId, this.LeaseInfo.BlobUri)
            match payload with
            | Single item -> return item
            | Batch items -> return items.[Option.get this.LeaseInfo.BatchIndex]
        }
        
        member this.Id : CloudWorkItemId = this.LeaseInfo.WorkItemId
        
        member this.WorkItemType : CloudWorkItemType = this.WorkItemType
        
        member this.Size : int64 = this.WorkItemSize
        
        member this.TargetWorker : IWorkerId option = 
            match this.LeaseInfo.TargetWorker with
            | None -> None
            | Some w -> Some(WorkerId(w) :> _)
        
        member this.Process : ICloudProcessEntry = 
            new CloudProcessEntry(this.ClusterId, this.LeaseInfo.ProcessId, this.ProcessInfo) :> _
        
        member this.Type : string = this.TypeName

    /// Creates a new WorkItemLeaseToken with supplied configuration parameters
    static member Create(clusterId : ClusterId, info : WorkItemLeaseTokenInfo, monitor : WorkItemLeaseMonitor, faultInfo : CloudWorkItemFaultInfo) = async {
        let! processRecordT = CloudProcessRecord.GetProcessRecord(clusterId, info.ProcessId) |> Async.StartChild
        let! workRecord = Table.read<WorkItemRecord> clusterId.StorageAccount clusterId.RuntimeTable info.ProcessId (fromGuid info.WorkItemId)
        let! processRecord = processRecordT

        return {
            ClusterId = clusterId
            CompleteAction = MarshaledAction.Create monitor.CompleteWith
            WorkItemSize = workRecord.GetSize()
            WorkItemType = workRecord.GetWorkItemType()
            TypeName = workRecord.Type
            FaultInfo = faultInfo
            LeaseInfo = info
            ProcessInfo = processRecord.ToCloudProcessInfo()
        }
    }