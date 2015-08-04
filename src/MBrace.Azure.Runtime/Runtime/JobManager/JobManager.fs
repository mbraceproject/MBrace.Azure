namespace MBrace.Azure.Runtime

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Azure
open System.Runtime.Serialization
open System

[<AutoSerializable(true); DataContract; Sealed>]  
type JobManager private (config : ConfigurationId, logger : ISystemLogger) =
    let [<DataMember(Name = "config")>] config = config
    let [<DataMember(Name = "logger")>] logger = logger

    let [<IgnoreDataMember>] mutable queue = Unchecked.defaultof<_>
    let [<IgnoreDataMember>] mutable topic = Unchecked.defaultof<_>
    let [<IgnoreDataMember>] mutable subscription = Unchecked.defaultof<_>
    let [<IgnoreDataMember>] mutable queueMessage : JobLeaseToken option ref = Unchecked.defaultof<_> // sorry
    let [<IgnoreDataMember>] mutable topicMessage : JobLeaseToken option ref = Unchecked.defaultof<_>

    [<OnDeserialized>]
    let init (_ : StreamingContext) =
        queue <- Async.RunSync(Queue.Create(config, logger))
        topic <- Async.RunSync(Topic.Create(config, logger))
        subscription <- None
        queueMessage <- ref None
        topicMessage <- ref None

    do init Unchecked.defaultof<_>

    let rec mkLoop (recv : Async<JobLeaseToken option>) (slot : JobLeaseToken option ref) =
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
                        return None
            }
            match newMessage with
            | None -> ()
            | Some m -> slot := Some m
            return! mkLoop recv slot
        }

    member this.SetLocalWorkerId(id : IWorkerId) =
        queue.LocalWorkerId <- id
        topic.LocalWorkerId <- id
        subscription <- Some(topic.GetSubscription(id))
        Async.Start(mkLoop (queue.TryDequeue()) queueMessage)
        Async.Start(mkLoop (subscription.Value.TryDequeue()) topicMessage)

    interface IJobQueue with
        member this.TryDequeue(id: IWorkerId): Async<ICloudJobLeaseToken option> = 
            async {
                let isDefault =
                    match subscription with
                    | None -> false
                    | Some s -> s.TargetWorkerId = id 

                let! jobToken = async {
                    match isDefault, queueMessage.Value, topicMessage.Value with
                    | true, (Some _ as m), _ -> queueMessage := None; return m
                    | true, _, (Some _ as m) -> topicMessage := None; return m
                    | true, None, None -> return None
                    | false, _, _ -> return! topic.GetSubscription(id).TryDequeue()
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

        member this.Enqueue(job: CloudJob): Async<unit> = 
            async {
                match job.TargetWorker with
                | Some _ -> return! topic.Enqueue(job)
                | None   -> return! queue.Enqueue(job)
            }

    static member Create(config : ConfigurationId, logger) = new JobManager(config, logger)