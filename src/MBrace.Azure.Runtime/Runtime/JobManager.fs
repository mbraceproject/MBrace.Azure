﻿namespace MBrace.Azure.Runtime

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Azure
open System.Runtime.Serialization
open System
open MBrace.Azure.Runtime.Utilities

[<AutoSerializable(true); DataContract>]  
type JobManager private (config : ConfigurationId) =
    let [<DataMember(Name = "config")>] config = config

    let [<IgnoreDataMember>] mutable queue = Unchecked.defaultof<_>
    let [<IgnoreDataMember>] mutable topic = Unchecked.defaultof<_>
    let [<IgnoreDataMember>] mutable subscription = Unchecked.defaultof<_>
    let [<IgnoreDataMember>] mutable queueMessage : JobLeaseToken option ref = Unchecked.defaultof<_> // sorry
    let [<IgnoreDataMember>] mutable topicMessage : JobLeaseToken option ref = Unchecked.defaultof<_>

    [<OnDeserialized>]
    let init _ =
        queue <- Async.RunSync(Queue.Create(config))
        topic <- Async.RunSync(Topic.Create(config))
        subscription <- None
        queueMessage <- ref None
        topicMessage <- ref None

    do init ()

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
                    | Choice2Of2 e -> return None
            }
            match newMessage with
            | None -> ()
            | Some m -> slot := Some m
            return! mkLoop recv slot
        }

    member this.SetDefaultWorker(id : IWorkerId) =
        subscription <- Some(topic.GetSubscription(id))

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

                let! jobToken = async {
                    match isDefault, queueMessage.Value, topicMessage.Value with
                    | true, (Some _ as m), _ -> queueMessage := None; return m
                    | true, _, (Some _ as m) -> topicMessage := None; return m
                    | true, None, None -> return None
                    | false, _, _ -> return! topic.GetSubscription(id).TryDequeue()
                }

                match jobToken with
                | None -> return None
                | Some token ->
                    let record = new JobRecord(token.ParentJobId, token.Id)
                    record.DequeueTime <- nullable token.DequeueTime
                    record.Status <- nullable(int JobStatus.Dequeued)
                    record.CurrentWorker <- subscription.Value.WorkerId.Id
                    record.DeliveryCount <- nullable token.DeliveryCount
                    let! _record = Table.merge config config.RuntimeTable record
                    return Some(token :> ICloudJobLeaseToken)
            }

        member this.BatchEnqueue(jobs: CloudJob []): Async<unit> = 
            async {
                if jobs.Length > 1024 then raise(ArgumentException(sprintf "Max batch size reached : %d/1024" jobs.Length))
                let nQueue = jobs |> Seq.sumBy (fun j -> Convert.ToInt32 j.TargetWorker.IsSome)
                if nQueue <> jobs.Length || nQueue <> 0 then
                    raise(NotSupportedException("Jobs with mixed TargetWorker are not supported."))

                let records = jobs |> Seq.map JobRecord.FromCloudJob
                do! Table.insertBatch config config.RuntimeTable records
                let! metadata =
                    if nQueue = jobs.Length then
                        queue.EnqueueBatch(jobs)
                    else
                        topic.EnqueueBatch(jobs)
                let newRecords = 
                    records |> Seq.mapi (fun i r -> 
                        let newRec = r.CloneDefault()
                        newRec.Status <- nullable(int JobStatus.Enqueued)
                        newRec.EnqueueTime <- nullable r.Timestamp
                        newRec.Size <- nullable(Seq.nth i metadata)
                        newRec)
                do! Table.mergeBatch config config.RuntimeTable newRecords
            }

        member this.Enqueue(job: CloudJob): Async<unit> = 
            async {
                let record = JobRecord.FromCloudJob(job)
                do! Table.insert config config.RuntimeTable record
                let! metadata = 
                    match job.TargetWorker with
                    | Some _ -> topic.Enqueue(job)
                    | None   -> queue.Enqueue(job)
                let newRecord = record.CloneDefault()
                newRecord.Status <- nullable(int JobStatus.Enqueued)
                newRecord.EnqueueTime <- nullable record.Timestamp
                newRecord.Size <- nullable metadata
                let! _ = Table.merge config config.RuntimeTable newRecord
                return ()
            }

    static member Create(config : ConfigurationId) = new JobManager(config)