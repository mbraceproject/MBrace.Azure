namespace MBrace.Azure.Runtime

open System

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Azure

module WorkerSubscription =

    /// Disposable MBrace worker subscription state object
    [<NoEquality; NoComparison; AutoSerializable(false)>]
    type Subscription =
        {
            Manager : ClusterManager
            Agent : WorkerAgent
            WorkItemEvaluator : ICloudWorkItemEvaluator
            StoreLogger : IRemoteSystemLogger
            StoreLoggerSubscription : IDisposable
        }
    with
        member s.Dispose() =
            Disposable.dispose s.Agent
            Disposable.dispose s.WorkItemEvaluator
            Disposable.dispose s.StoreLoggerSubscription
            Disposable.dispose s.StoreLogger

    let initialize (config : Configuration) (workerId : string) (logger : ISystemLogger) 
                    (heartbeatInterval : TimeSpan) (heartbeatThreshold : TimeSpan)
                    (useAppDomainIsolation : bool) (maxConcurrentWorkItems : int) 
                    (customResources : ResourceRegistry) =
        async {
            logger.LogInfof "Initializing worker %A" workerId
            let workerId = new WorkerId(workerId) :> IWorkerId

            logger.LogInfof "Creating ClusterManager"
            let! clusterManager = ClusterManager.Create(config, customResources, logger)
            let runtimeManager = clusterManager :> IRuntimeManager

            logger.LogInfof "Initializing table store system logger"
            let! storeLogger = runtimeManager.RuntimeSystemLogManager.CreateLogWriter workerId
            let storeLoggerSubscription = runtimeManager.LocalSystemLogManager.AttachLogger storeLogger

            let jobEvaluator = 
                if useAppDomainIsolation then
                    logger.LogInfof "Initializing AppDomain pool evaluator"
                    let marshalledLogger = new MarshaledLogger(runtimeManager.SystemLogger)
                    let workingDirectory = ProcessConfiguration.WorkingDirectory
                    let init () =
                        let domainName = System.AppDomain.CurrentDomain.FriendlyName
                        marshalledLogger.LogInfof "Initializing Application Domain %A" domainName
                        ProcessConfiguration.InitAsWorkerSlaveDomain workingDirectory
                        let _ = ClusterId.Activate config // ensure connection strings are loaded in AppDomain
                        ()

                    let managerF () =
                        marshalledLogger.LogInfof "Initializing AppDomain ClusterManager" 
                        let manager = ClusterManager.Create(config, customResources) |> Async.RunSync :> IRuntimeManager
                        // avoid exporting cluster manager initialization logs for each AppDomain, 
                        // attach marshalled logger a posteriori
                        let _ = manager.LocalSystemLogManager.AttachLogger marshalledLogger
                        manager, workerId

                    AppDomainWorkItemEvaluator.Create(managerF, init) :> ICloudWorkItemEvaluator
                else
                    logger.LogInfo "Initializing local workItem evaluator"
                    LocalWorkItemEvaluator.Create(runtimeManager, workerId) :> ICloudWorkItemEvaluator

            logger.LogInfo "Creating worker subscription"
            do clusterManager.WorkItemManager.SetLocalWorkerId workerId // TODO: this is ugly; need to fix
            logger.LogInfo "Creating worker agent"
            let! agent = WorkerAgent.Create(runtimeManager, workerId, jobEvaluator, maxConcurrentWorkItems, 
                                            submitPerformanceMetrics = true, heartbeatInterval = heartbeatInterval, heartbeatThreshold = heartbeatThreshold)

            logger.LogInfo "Starting worker agent"
            do! agent.Start()
            logger.LogInfo "Worker agent started"
            return {
                Agent = agent
                Manager = clusterManager
                StoreLoggerSubscription = storeLoggerSubscription
                StoreLogger = storeLogger
                WorkItemEvaluator = jobEvaluator
            }
        }
