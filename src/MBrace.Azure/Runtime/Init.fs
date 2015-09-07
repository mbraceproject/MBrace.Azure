namespace MBrace.Azure.Runtime

open MBrace.Azure
open MBrace.Runtime
open MBrace.Core.Internals
open MBrace.Core

[<Sealed>]
type internal Initializer =
    static member Init(config : Configuration,
                        workerId : string, 
                        logger : AttacheableLogger, 
                        useAppDomainIsolation : bool,
                        maxConcurrentJobs : int, 
                        customResources : ResourceRegistry) =
        async {
            logger.LogInfof "Initializing worker %A" workerId
            let workerId = new WorkerId(workerId) :> IWorkerId

            logger.LogInfof "Creating RuntimeManager"
            let runtimeManager = ClusterManager.CreateForWorker(config, workerId, logger, customResources)

            let jobEvaluator = 
                if useAppDomainIsolation then
                    logger.LogInfof "Initializing AppDomainpool evaluator"
                    let marshalledLogger = new MarshaledLogger(logger)
                    let init () =
                        let domainName = System.AppDomain.CurrentDomain.FriendlyName
                        marshalledLogger.LogInfof "Initializing Application Domain %A" domainName
                        Config.Activate(config, isClientInstance = false, populateDirs = false)
                        marshalledLogger.LogInfof "Configuration activated in AppDomain %A" domainName

                    let managerF () =
                        ClusterManager.CreateForAppDomain(config, workerId, marshalledLogger, customResources) :> IRuntimeManager , workerId

                    AppDomainJobEvaluator.Create(managerF, init) :> ICloudJobEvaluator
                else
                    logger.LogInfo "Initializing local job evaluator"
                    LocalJobEvaluator.Create(runtimeManager, workerId) :> ICloudJobEvaluator

            logger.LogInfo "Creating worker agent"
            let! agent = WorkerAgent.Create(runtimeManager, workerId, jobEvaluator, maxConcurrentJobs, submitPerformanceMetrics = true)
            logger.LogInfo "Starting worker agent"
            do! agent.Start()
            logger.LogInfo "Worker agent started"
            return agent
        }
