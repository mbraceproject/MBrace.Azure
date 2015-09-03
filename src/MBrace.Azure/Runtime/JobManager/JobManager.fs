namespace MBrace.Azure.Runtime

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Azure
open MBrace.Azure.Runtime.Utilities
open System.Runtime.Serialization
open System

[<AutoSerializable(true); DataContract; Sealed>]  
type JobManager private (config : ConfigurationId, workerManager : WorkerManager, logger : ISystemLogger) =
    let [<DataMember(Name = "config")>] config = config
    let [<DataMember(Name = "logger")>] logger = logger

    let [<IgnoreDataMember>] mutable queue = Unchecked.defaultof<_>
    let [<IgnoreDataMember>] mutable topic = Unchecked.defaultof<_>
    let [<IgnoreDataMember>] mutable subscription = Unchecked.defaultof<_>
    let [<IgnoreDataMember>] mutable queueMessage : JobLeaseToken option ref = Unchecked.defaultof<_> // sorry
    let [<IgnoreDataMember>] mutable topicMessage : JobLeaseToken option ref = Unchecked.defaultof<_>
    let [<IgnoreDataMember>] mutable maintenanceMessage : JobLeaseToken option ref = Unchecked.defaultof<_>

    [<OnDeserialized>]
    let init (_ : StreamingContext) =
        queue <- Async.RunSync(Queue.Create(config, logger))
        topic <- Async.RunSync(Topic.Create(config, logger))
        subscription <- None
        queueMessage <- ref None
        topicMessage <- ref None
        maintenanceMessage <- ref None

    do init Unchecked.defaultof<_>

    let rec mkLoop (recv : Async<JobLeaseToken option>) (slot : JobLeaseToken option ref) : Async<unit> =
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

    /// Job queue maintenance : check for non responsive workers and cleanup their queu
    let rec cleanup () = async {
        do! Async.Sleep(int(0.5 * WorkerManager.MaxHeartbeatTimespan.TotalMilliseconds))
        let! result = Async.Catch <| async {
            logger.LogInfof "JobManager : performing cleanup"
            let! workersToCheck = workerManager.GetInactiveWorkers()
            let level = if workersToCheck.Length > 0 then LogLevel.Warning else LogLevel.Info
            logger.Logf level "JobManager : found %d inactive workers." workersToCheck.Length
            for worker in workersToCheck do
                let workerSubscription = topic.GetSubscription(worker.Id)
                logger.LogInfof "JobManager : checking worker %A queue." worker.Id
                // process at most 'messageCount' messages.
                // spin until maintainance message slot is free
                let rec loop flag = async {
                    if flag then
                        match maintenanceMessage.Value with
                        | Some _ ->
                            do! Async.Sleep 20
                            return! loop flag
                        | None ->
                            let! message = workerSubscription.TryDequeue()
                            if message.IsSome then logger.LogInfof "JobManager : dequeued message for worker %A" worker.Id
                            maintenanceMessage := message
                            return! loop message.IsSome
                }

                do! loop true
            }

        match result with
        | Choice1Of2 _ -> logger.LogInfo "JobManager : maintenance complete."
        | Choice2Of2 ex -> logger.Logf LogLevel.Error "JobManager : maintenance error:  %A" ex

        return! cleanup ()   
    }

    /// Set job queue affinity and start background tasks.
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


    interface ICloudJobQueue with
        member this.TryDequeue(id: IWorkerId): Async<ICloudJobLeaseToken option> = 
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
                | Some token -> return Some(token :> ICloudJobLeaseToken)
            }

        member this.BatchEnqueue(jobs: CloudJob []): Async<unit> = 
            async {
                if jobs.Length > 1024 then 
                    raise(NotSupportedException(sprintf "Max batch size reached : %d/1024" jobs.Length))
                let nQueue = jobs |> Seq.sumBy (fun j -> Convert.ToInt32 j.TargetWorker.IsNone)
                if nQueue <> jobs.Length && nQueue <> 0 then
                    raise(NotSupportedException("Jobs with mixed TargetWorker are not supported."))
                let parentId = (Seq.head jobs).TaskEntry.Id
                if jobs |> Seq.exists(fun j -> j.TaskEntry.Id <> parentId) then
                    raise(NotSupportedException("Jobs with different parent TaskEntry not supported."))

                if jobs.Length = 0 then
                    return ()
                elif nQueue = jobs.Length then
                    return! queue.EnqueueBatch(jobs)
                else
                    return! topic.EnqueueBatch(jobs)
            }

        member this.Enqueue(job: CloudJob, isClientSideEnqueue : bool): Async<unit> = 
            async {
                match job.TargetWorker with
                | Some _ -> return! topic.Enqueue(job, allowNewSifts = isClientSideEnqueue)
                | None   -> return! queue.Enqueue(job, allowNewSifts = isClientSideEnqueue)
            }

    static member Create(config : ConfigurationId, workerManager, logger) = new JobManager(config, workerManager, logger)