namespace MBrace.Azure.Runtime

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Azure
open MBrace.Azure.Runtime.Utilities
open System.Runtime.Serialization
open System

// TODO : make non-serializable & thread safe

[<AutoSerializable(true); DataContract; Sealed>]  
type WorkItemManager private (config : ClusterId, workerManager : WorkerManager, logger : ISystemLogger) =
    let [<DataMember(Name = "config")>] config = config
    let [<DataMember(Name = "logger")>] logger = logger

    let [<IgnoreDataMember>] mutable queue = Unchecked.defaultof<_>
    let [<IgnoreDataMember>] mutable topic = Unchecked.defaultof<_>
    let [<IgnoreDataMember>] mutable subscription = Unchecked.defaultof<_>
    let [<IgnoreDataMember>] mutable queueMessage : WorkItemLeaseToken option ref = Unchecked.defaultof<_> // sorry
    let [<IgnoreDataMember>] mutable topicMessage : WorkItemLeaseToken option ref = Unchecked.defaultof<_>
    let [<IgnoreDataMember>] mutable maintenanceMessage : WorkItemLeaseToken option ref = Unchecked.defaultof<_>

    [<OnDeserialized>]
    let init (_ : StreamingContext) =
        queue <- Async.RunSync(Queue.Create(config, logger))
        topic <- Async.RunSync(Topic.Create(config, logger))
        subscription <- None
        queueMessage <- ref None
        topicMessage <- ref None
        maintenanceMessage <- ref None

    do init Unchecked.defaultof<_>

    let rec mkLoop (recv : Async<WorkItemLeaseToken option>) (slot : WorkItemLeaseToken option ref) : Async<unit> =
        async {
            let! newMessage = async {
                match slot.Value with
                | Some _ -> 
                    do! Async.Sleep(20)
                    return None
                | None -> 
                    let! res = Async.Catch recv
                    match res with
                    | Choice1Of2 m -> return m
                    | Choice2Of2 e -> 
                        logger.Logf LogLevel.Error "Async receive loop error %A" e
                        do! Async.Sleep 1000
                        return None
            }
            match newMessage with
            | None -> ()
            | Some m -> slot := Some m
            return! mkLoop recv slot
        }

    /// WorkItem queue maintenance : check for non responsive workers and cleanup their queu
    let rec cleanup () = async {
        do! Async.Sleep(int(0.5 * WorkerManager.MaxHeartbeatTimespan.TotalMilliseconds))
        let! result = Async.Catch <| async {
            logger.LogInfof "WorkItemManager : performing maintenance."
            let! workersToCheck = workerManager.GetInactiveWorkers()
            let workersToCheck =
                workersToCheck
                |> Array.sortBy (fun w -> w.LastHeartbeat)
                |> Array.rev
            let level = if workersToCheck.Length > 0 then LogLevel.Warning else LogLevel.Info
            logger.Logf level "WorkItemManager : found %d inactive workers." workersToCheck.Length
            for worker in workersToCheck do
                let workerSubscription = topic.GetSubscription(worker.Id)
                logger.LogInfof "WorkItemManager : checking worker %A queue." worker.Id
                let rec loop flag retry = async {
                    if flag || retry < 20 then
                        match maintenanceMessage.Value with
                        | Some _ ->
                            do! Async.Sleep 20
                            return! loop flag retry
                        | None ->
                            let! message = workerSubscription.TryDequeue()
                            maintenanceMessage := message
                            if message.IsSome then 
                                logger.LogInfof "WorkItemManager : dequeued message for worker %A" worker.Id
                                return! loop true 0
                            else
                                return! loop false (retry + 1)
                }

                do! loop true 0
            }

        match result with
        | Choice1Of2 _ -> logger.LogInfo "WorkItemManager : maintenance complete."
        | Choice2Of2 ex -> logger.Logf LogLevel.Error "WorkItemManager : maintenance error:  %A" ex

        return! cleanup ()   
    }

    /// Set workItem queue affinity and start background tasks.
    member this.SetLocalWorkerId(id : IWorkerId) =
        let _ = Validate.subscriptionName id.Id
        queue.LocalWorkerId <- id
        topic.LocalWorkerId <- id
        subscription <- Some(topic.GetSubscription(id))
        Async.Start(mkLoop (queue.TryDequeue()) queueMessage)
        Async.Start(mkLoop (subscription.Value.TryDequeue()) topicMessage)
        Async.Start(cleanup ())

    member this.GlobalQueueMessageCount = queue.MessageCount

    member this.WorkerQueueMessageCount(id : IWorkerId) =
        async {
            let subscription = topic.GetSubscription(id)
            return subscription.MessageCount
        }


    interface ICloudWorkItemQueue with
        member this.TryDequeue(id: IWorkerId): Async<ICloudWorkItemLeaseToken option> = 
            async {
                let isDefault =
                    match subscription with
                    | None -> false
                    | Some s -> s.TargetWorkerId = id 

                let! jobToken = async {
                    match isDefault, maintenanceMessage.Value, queueMessage.Value, topicMessage.Value with
                    | true, (Some _ as m), _, _ -> maintenanceMessage := None; return m
                    | true, _, (Some _ as m), _ -> queueMessage := None; return m
                    | true, _, _, (Some _ as m) -> topicMessage := None; return m
                    | true, None, None, None -> return None
                    | false, _, _, _ -> return! topic.GetSubscription(id).TryDequeue()
                }
                
                match jobToken with
                | None -> return None
                | Some token -> return Some(token :> ICloudWorkItemLeaseToken)
            }

        member this.BatchEnqueue(jobs: CloudWorkItem []): Async<unit> = 
            async {
                if jobs.Length > 1024 then 
                    raise(NotSupportedException(sprintf "Max batch size reached : %d/1024" jobs.Length))
                let nQueue = jobs |> Seq.sumBy (fun j -> Convert.ToInt32 j.TargetWorker.IsNone)
                if nQueue <> jobs.Length && nQueue <> 0 then
                    raise(NotSupportedException("WorkItems with mixed TargetWorker are not supported."))
                let parentId = (Seq.head jobs).Process.Id
                if jobs |> Seq.exists(fun j -> j.Process.Id <> parentId) then
                    raise(NotSupportedException("WorkItems with different parent Process not supported."))

                if jobs.Length = 0 then
                    return ()
                elif nQueue = jobs.Length then
                    return! queue.EnqueueBatch(jobs)
                else
                    return! topic.EnqueueBatch(jobs)
            }

        member this.Enqueue(workItem: CloudWorkItem, isClientSideEnqueue : bool): Async<unit> = 
            async {
                match workItem.TargetWorker with
                | Some _ -> return! topic.Enqueue(workItem, allowNewSifts = isClientSideEnqueue)
                | None   -> return! queue.Enqueue(workItem, allowNewSifts = isClientSideEnqueue)
            }

    static member Create(config : ClusterId, workerManager, logger) = new WorkItemManager(config, workerManager, logger)