namespace MBrace.Azure.Runtime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils.PrettyPrinters
open MBrace.Azure
open System.Runtime.Serialization
open System

[<AutoSerializable(true); DataContract>]  
type JobManager private (config : ConfigurationId) =
    let [<DataMember(Name = "config")>] config = config

    let [<IgnoreDataMember>] mutable queue = Unchecked.defaultof<_>
    let [<IgnoreDataMember>] mutable topic = Unchecked.defaultof<_>
    let [<IgnoreDataMember>] mutable subscription = Unchecked.defaultof<_>
    let [<IgnoreDataMember>] mutable queueMessage : ICloudJobLeaseToken option ref = Unchecked.defaultof<_> // sorry
    let [<IgnoreDataMember>] mutable topicMessage : ICloudJobLeaseToken option ref = Unchecked.defaultof<_>

    [<OnDeserialized>]
    let init _ =
        queue <- Async.RunSync(Queue.Create(config))
        topic <- Async.RunSync(Topic.Create(config))
        subscription <- None
        queueMessage <- ref None
        topicMessage <- ref None

    do init ()

    let rec mkLoop (recv : Async<ICloudJobLeaseToken option>) (slot : ICloudJobLeaseToken option ref) =
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
                    | Choice2Of2 e -> return None
            }
            match newMessage with
            | None -> ()
            | Some m -> slot := Some m
            return! mkLoop recv slot
        }

    interface IJobQueue with
        member this.TryDequeue(id: IWorkerId): Async<ICloudJobLeaseToken option> = 
            async {
                let isDefault =
                    match subscription with
                    | None -> 
                        subscription <- Some(topic.GetSubscription(id))
                        Async.Start(mkLoop (queue.TryDequeue()) queueMessage)
                        Async.Start(mkLoop (subscription.Value.TryDequeue()) topicMessage)
                        true
                    | Some s -> s.WorkerId = id 

                match isDefault, queueMessage.Value, topicMessage.Value with
                | true, (Some _ as m), _ -> queueMessage := None; return m
                | true, _, (Some _ as m) -> topicMessage := None; return m
                | true, None, None -> return None
                | false, _, _ -> return! topic.GetSubscription(id).TryDequeue()
            }

        member this.BatchEnqueue(jobs: CloudJob []): Async<unit> = 
            async {
                if jobs.Length > 1024 then raise(ArgumentException(sprintf "Max batch size reached : %d/1024" jobs.Length))
                let nQueue = jobs |> Seq.sumBy (fun j -> Convert.ToInt32 j.TargetWorker.IsSome)
                if nQueue <> jobs.Length || nQueue <> 0 then
                    raise(NotSupportedException("Jobs with mixed TargetWorker are not supported."))

                if nQueue = jobs.Length then
                    return! queue.EnqueueBatch(jobs)
                else
                    return! topic.EnqueueBatch(jobs)
            }

        member this.Enqueue(job: CloudJob): Async<unit> = 
            async {
                return! match job.TargetWorker with
                        | Some _ -> topic.Enqueue(job)
                        | None   -> queue.Enqueue(job)
            }

    static member Create(config : ConfigurationId) = new JobManager(config)