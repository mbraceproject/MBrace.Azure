namespace MBrace.Azure.Runtime

open System
open System.Threading

open Microsoft.ServiceBus.Messaging

open MBrace.Runtime
open MBrace.Runtime.Utils

/// TopicMonitor implements an agent which periodically checks all service bus topic subscriptions
/// for messages assigned to inactive workers. If found, it will push the messages back to the main
/// Queue, to be further processed by a different worker for fault handling.
[<Sealed; AutoSerializable(false)>]
type TopicMonitor private (workerManager : WorkerManager, topic : Topic, queue : Queue, logger : ISystemLogger) =
    let random =
        let seed = System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(obj())
        new Random(seed)

    // keeps a rough track of the current active cluster size
    let clusterSize = CacheAtom.Create(async { let! ws = workerManager.GetAllWorkers() in return ws.Length }, intervalMilliseconds = 10000)

    let cleanupWorkerQueue (worker : IWorkerId) = async {
        let subscription = topic.GetSubscription(worker)
        let! allMessages = subscription.DequeueAllMessagesBatch()
        if not <| Array.isEmpty allMessages then
            logger.LogInfof "TopicMonitor : Perfoming worker queue maintance for %A." worker.Id
            let cloneMsg (m : BrokeredMessage) =
                let m' = m.Clone()
                // keep the current delivery count as a separate property
                // as the message is reposted in the main queue
                m'.Properties.[Settings.TopicDeliveryCount] <- m.DeliveryCount - 1
                m'

            // clone messages and re-enqueue to main work item queue
            let newMessages = allMessages |> Array.map cloneMsg
            do! queue.EnqueueMessagesBatch(newMessages)

            // now that enqueue is complete, complete topic messages
            do! 
                allMessages 
                |> Seq.map (fun m -> async { do! m.CompleteAsync() })
                |> Async.Parallel 
                |> Async.Ignore

            return true
        else
            return false
    }

    // WorkItem queue maintenance : periodically check for non-responsive workers and cleanup their queue
    let rec loop () = async {
        // perform cleanup every 20 seconds with a probability of 1/N,
        // where N is the current active cluster size
        do! Async.Sleep 20000
        if random.Next(0, clusterSize.Value) <> 0 then return! loop() else

        let! result = Async.Catch <| async {
            let! workersToCheck = workerManager.GetInactiveWorkers()
            let! results = workersToCheck |> Seq.map (fun w -> cleanupWorkerQueue w.Id) |> Async.Parallel
            return results |> Array.exists id
        }

        match result with
        | Choice1Of2 true -> logger.LogInfo "TopicMonitor : maintenance complete."
        | Choice1Of2 _ -> ()
        | Choice2Of2 ex -> logger.Logf LogLevel.Error "TopicMonitor : maintenance error:  %A" ex

        return! loop ()   
    }

    let cts = new CancellationTokenSource()
    do Async.Start(loop(), cts.Token)

    interface IDisposable with
        member __.Dispose() = cts.Cancel()

    static member Create(config : ClusterId, workerManager : WorkerManager, logger : ISystemLogger) = async {
        let! queueT = Queue.Create(config, logger) |> Async.StartChild
        let! topic = Topic.Create(config, logger)
        let! queue = queueT
        return new TopicMonitor(workerManager, topic, queue, logger)
    }