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
    let clusterSize = 
        let getter = async { try let! ws = workerManager.GetAvailableWorkers() in return ws.Length with _ -> return 2 }
        CacheAtom.Create(getter, intervalMilliseconds = 10000)

    let cleanupWorkerQueue (worker : IWorkerId) = async {
        let subscription = topic.GetSubscription(worker)
        let! allMessages = subscription.DequeueAllMessagesBatch()
        if not <| Array.isEmpty allMessages then
            logger.LogInfof "TopicMonitor : Perfoming worker queue maintance for %A." worker.Id
            let cloneMsg (m : BrokeredMessage) =
                let m' = m.Clone()
                // keep the current delivery count as a separate property
                // as the message is reposted in the main queue
                m'.Properties.[ServiceBusSettings.TopicDeliveryCount] <- m.DeliveryCount - 1
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
    }

    // WorkItem queue maintenance : periodically check for non-responsive workers and cleanup their queue
    let rec loop () = async {
        do! Async.Sleep 10000
        if random.Next(0, min clusterSize.Value 4) = 0 then return! loop() else

        logger.LogInfo "TopicMonitor : starting topic maintenance."

        let! result = Async.Catch <| async {
            let! workersToCheck = workerManager.GetInactiveWorkers()
            do! workersToCheck |> Seq.map (fun w -> cleanupWorkerQueue w.Id) |> Async.Parallel |> Async.Ignore
        }

        match result with
        | Choice1Of2 () -> logger.LogInfo "TopicMonitor : maintenance complete."
        | Choice2Of2 ex -> logger.Logf LogLevel.Error "TopicMonitor : maintenance error:  %A" ex

        return! loop ()   
    }

    let cts = new CancellationTokenSource()
    do Async.Start(loop(), cts.Token)

    interface IDisposable with
        member __.Dispose() = cts.Cancel()

    static member Create(clusterId : ClusterId, workerManager : WorkerManager, logger : ISystemLogger) = async {
        let! queueT = Queue.Create(clusterId, logger) |> Async.StartChild
        let! topic = Topic.Create(clusterId, logger)
        let! queue = queueT
        return new TopicMonitor(workerManager, topic, queue, logger)
    }