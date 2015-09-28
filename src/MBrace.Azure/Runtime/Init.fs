namespace MBrace.Azure.Runtime

open MBrace.Azure
open MBrace.Runtime
open MBrace.Core.Internals
open MBrace.Core

[<Sealed>]
type internal Initializer =
    static member Init(config : ClusterConfiguration,
                        workerId : string, 
                        logger : AttacheableLogger, 
                        useAppDomainIsolation : bool,
                        maxConcurrentWorkItems : int, 
                        customResources : ResourceRegistry) =
        async {
            logger.LogInfof "Initializing worker %A" workerId
            let workerId = new WorkerId(workerId) :> IWorkerId

            logger.LogInfof "Creating RuntimeManager"
            let runtimeManager = ClusterManager.CreateForWorker(config, workerId, logger, customResources)

            let jobEvaluator = 
                if useAppDomainIsolation then
                    logger.LogInfof "Initializing AppDomain pool evaluator"
                    let marshalledLogger = new MarshaledLogger(logger)
                    let workingDirectory = ProcessConfiguration.WorkingDirectory
                    let init () =
                        let domainName = System.AppDomain.CurrentDomain.FriendlyName
                        marshalledLogger.LogInfof "Initializing Application Domain %A" domainName
                        ProcessConfiguration.InitAsWorkerSlaveDomain workingDirectory

                    let managerF () =
                        ClusterManager.CreateForAppDomain(config, workerId, marshalledLogger, customResources) :> IRuntimeManager , workerId

                    AppDomainWorkItemEvaluator.Create(managerF, init) :> ICloudWorkItemEvaluator
                else
                    logger.LogInfo "Initializing local workItem evaluator"
                    LocalWorkItemEvaluator.Create(runtimeManager, workerId) :> ICloudWorkItemEvaluator

            logger.LogInfo "Creating worker agent"
            let! agent = WorkerAgent.Create(runtimeManager, workerId, jobEvaluator, maxConcurrentWorkItems, submitPerformanceMetrics = true)
            logger.LogInfo "Starting worker agent"
            do! agent.Start()
            logger.LogInfo "Worker agent started"
            return agent
        }
