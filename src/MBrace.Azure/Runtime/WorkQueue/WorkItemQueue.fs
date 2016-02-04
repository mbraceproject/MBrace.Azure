namespace MBrace.Azure.Runtime

open System
open System.Runtime.Serialization
open System.Threading

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Azure
open MBrace.Azure.Runtime.Utilities

/// Implements MBrace.Runtime.IWorkItemQueue
[<AutoSerializable(false); Sealed>]  
type WorkItemQueue private (queue : Queue, topic : Topic) =

    interface ICloudWorkItemQueue with
        member this.TryDequeue(id: IWorkerId): Async<ICloudWorkItemLeaseToken option> = async {
            let! subscription = topic.GetSubscription(id)

            // first attempt dequeueing from topic, before attempting queue
            let! leaseToken = subscription.TryDequeue(id)
            match leaseToken with
            | Some _ -> return leaseToken
            | None -> return! queue.TryDequeue(id)
        }

        member this.BatchEnqueue(jobs: CloudWorkItem []): Async<unit> = async {
            if jobs.Length = 0 then ()
            elif jobs.Length > 1024 then
                raise <| ArgumentOutOfRangeException(sprintf "Max batch size reached : %d/1024" jobs.Length)
            else
                let nQueue = jobs |> Array.sumBy (fun j -> match j.TargetWorker with None -> 1 | _ -> 0)
                if nQueue = jobs.Length then return! queue.EnqueueBatch(jobs)
                elif nQueue = 0 then return! topic.EnqueueBatch(jobs)
                else
                    // TODO : fix this; future MBrace.Core versions will feature mixed modes
                    raise <| NotSupportedException("WorkItems with mixed TargetWorker are not supported.")
        }

        member this.Enqueue(workItem: CloudWorkItem, isClientSideEnqueue : bool): Async<unit> = async {
            match workItem.TargetWorker with
            | Some _ -> return! topic.Enqueue(workItem, allowNewSifts = isClientSideEnqueue)
            | None   -> return! queue.Enqueue(workItem, allowNewSifts = isClientSideEnqueue)
        }

    static member Create(clusterId : ClusterId, logger : ISystemLogger) = async {
        let! queueT = Queue.Create(clusterId, logger) |> Async.StartChild
        let! topic = Topic.Create(clusterId, logger)
        let! queue = queueT
        return new WorkItemQueue(queue, topic)
    }