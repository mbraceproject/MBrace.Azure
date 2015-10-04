namespace MBrace.Azure.Runtime

open System
open System.IO
open System.Threading
open System.Runtime.Serialization

open Microsoft.ServiceBus.Messaging

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Azure.Runtime.Utilities

/// Info stored in BrokeredMessage.
type internal WorkItemLeaseTokenInfo =
    {
        ConfigurationId : ClusterId
        WorkItemId : Guid
        BlobUri : string
        MessageLockId : Guid
        ProcessId : string
        BatchIndex : int option
        TargetWorker : string option
        DeliveryCount : int
        DequeueTime : DateTimeOffset
    }
with
    override this.ToString() = sprintf "leaseinfo:%A" this.WorkItemId

[<Sealed; AbstractClass>]
type internal WorkItemLeaseMonitor = 
    // TODO : rewrite as object that is encapsulated in leaseToken
    static member Start(message : BrokeredMessage, token : WorkItemLeaseTokenInfo, 
                            logger : ISystemLogger, ?cancellationToken : CancellationToken) = 

        let rec renewLoop() = async { 
            // NOTE : WorkItemLeaseMonitor Complete/Abandon should
            // cause RenewLock to raise a MessageLostException, but this doesn't happen.
            // As a workaround we stop the renewLoop when the table storage record .Complete is true.
            let! renewResult = 
                async {
                    do! message.RenewLockAsync()
                    let now = DateTimeOffset.Now
                    let! record = Table.read<WorkItemRecord> token.ConfigurationId.StorageAccount token.ConfigurationId.RuntimeTable token.ProcessId (fromGuid token.WorkItemId)
                    match record.Completed, record.Status with
                    | Nullable true, Nullable status when status = int WorkItemStatus.Completed || status = int WorkItemStatus.Faulted ->
                        return true
                    | _ ->
                        let updated = record.CloneDefault()
                        updated.ETag <- "*"
                        updated.RenewLockTime <- nullable now
                        let! _updated = Table.merge token.ConfigurationId.StorageAccount token.ConfigurationId.RuntimeTable updated
                        return false
                } |> Async.Catch 

            match renewResult with
            | Choice1Of2 false ->
                logger.Logf LogLevel.Debug "%A : lock renewed" token
                do! Async.Sleep ServiceBusSettings.RenewLockInverval
                return! renewLoop()
            | Choice1Of2 true ->
                logger.Logf LogLevel.Info "%A : completed" token
                message.Dispose()
            | Choice2Of2 ex when (ex :? MessageLockLostException) -> 
                logger.Logf LogLevel.Warning "%A : lock lost" token
                message.Dispose()
            | Choice2Of2 ex ->
                logger.LogErrorf "%A : lock renew failed with %A" token ex
                do! Async.Sleep ServiceBusSettings.RenewLockInverval
                return! renewLoop()                                    
        }
            
        Async.Start(renewLoop(), ?cancellationToken = cancellationToken)
    
    static member Complete(token : WorkItemLeaseTokenInfo) : Async<unit> = async { 
        let config = token.ConfigurationId
        match token.TargetWorker with
        | None -> 
            let queue = config.ServiceBusAccount.CreateQueueClient(config.RuntimeQueue, ReceiveMode.PeekLock)
            return! queue.CompleteAsync(token.MessageLockId)
        | Some affinity -> 
            let subscription = config.ServiceBusAccount.CreateSubscriptionClient(config.RuntimeTopic, affinity)
            return! subscription.CompleteAsync(token.MessageLockId)
    }

    static member Abandon(token : WorkItemLeaseTokenInfo) : Async<unit> = async { 
        let config = token.ConfigurationId
        match token.TargetWorker with
        | None -> 
            let queue = config.ServiceBusAccount.CreateQueueClient(config.RuntimeQueue, ReceiveMode.PeekLock)
            return! queue.AbandonAsync(token.MessageLockId)
        | Some affinity -> 
            let subscription = config.ServiceBusAccount.CreateSubscriptionClient(config.RuntimeTopic, affinity)
            return! subscription.AbandonAsync(token.MessageLockId)
    }

[<AutoSerializable(true); DataContract>]
type WorkItemLeaseToken internal (info : WorkItemLeaseTokenInfo, faultInfo : CloudWorkItemFaultInfo, procInfo : CloudProcessInfo)  = 
    let [<DataMember(Name="info")>] info = info
    let [<DataMember(Name="faultInfo")>] faultInfo = faultInfo
    let [<DataMember(Name="processInfo")>] procInfo = procInfo
    let [<IgnoreDataMember>] mutable record : WorkItemRecord = null
        
    let init () =
        record <- Async.RunSync(Table.read info.ConfigurationId.StorageAccount info.ConfigurationId.RuntimeTable info.ProcessId (fromGuid info.WorkItemId))

    [<OnDeserialized>]
    let _onDeserialized (_ : StreamingContext) = init ()

    do init ()

    member internal this.Info = info

    override this.ToString () = sprintf "lease:%A" info.WorkItemId

    interface ICloudWorkItemLeaseToken with
        member this.DeclareCompleted() : Async<unit> = async {
            do! WorkItemLeaseMonitor.Complete(info)
            let record = new WorkItemRecord(info.ProcessId, fromGuid info.WorkItemId)
            record.Status <- nullable(int WorkItemStatus.Completed)
            record.CompletionTime <- nullable(DateTimeOffset.Now)
            record.Completed <- nullable true
            record.ETag <- "*" 
            let! _record = Table.merge info.ConfigurationId.StorageAccount info.ConfigurationId.RuntimeTable record
            return ()
        }
        
        member this.DeclareFaulted(edi : ExceptionDispatchInfo) : Async<unit> = async { 
            do! WorkItemLeaseMonitor.Abandon(info) // TODO : should this be Abandon or Complete?
            let record = new WorkItemRecord(info.ProcessId, fromGuid info.WorkItemId)
            record.Status <- nullable(int WorkItemStatus.Faulted)
            record.Completed <- nullable false
            record.CompletionTime <- nullableDefault
            // there exists a remote possibility that fault exceptions might be of arbitrary size
            // should probably persist payload to blob as done with results
            record.LastException <- ProcessConfiguration.Serializer.Pickle edi
            record.FaultInfo <- nullable(int FaultInfo.FaultDeclaredByWorker)
            record.ETag <- "*"
            let! _record = Table.merge info.ConfigurationId.StorageAccount info.ConfigurationId.RuntimeTable record
            return () 
        }
        
        member this.FaultInfo : CloudWorkItemFaultInfo = faultInfo
        
        member this.GetWorkItem() : Async<CloudWorkItem> = async { 
            let! payload = BlobPersist.ReadPersistedClosure<MessagePayload>(info.ConfigurationId, info.BlobUri)
            match payload with
            | Single item -> return item
            | Batch items -> return items.[Option.get info.BatchIndex]
        }
        
        member this.Id : CloudWorkItemId = info.WorkItemId
        
        member this.WorkItemType : CloudWorkItemType =
            let jobKind = enum<WorkItemKind>(record.Kind.GetValueOrDefault(-1))
            match jobKind with
            | WorkItemKind.ProcessRoot -> ProcessRoot
            | WorkItemKind.Choice   -> ChoiceChild(record.Index.GetValueOrDefault(-1), record.MaxIndex.GetValueOrDefault(-1))
            | WorkItemKind.Parallel -> ParallelChild(record.Index.GetValueOrDefault(-1), record.MaxIndex.GetValueOrDefault(-1))
            | _ -> failwithf "Invalid WorkItemKind %d" <| int jobKind
        
        member this.Size : int64 = record.Size.GetValueOrDefault(-1L)
        
        member this.TargetWorker : IWorkerId option = 
            match info.TargetWorker with
            | None -> None
            | Some w -> Some(WorkerId(w) :> _)
        
        member this.Process : ICloudProcessEntry = CloudProcessEntry(info.ConfigurationId, info.ProcessId, procInfo) :> _
        
        member this.Type : string = record.Type