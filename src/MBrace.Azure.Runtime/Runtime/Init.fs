namespace MBrace.Azure.Runtime

open MBrace.Azure
open MBrace.Runtime
open MBrace.Core.Internals
open MBrace.Core

[<Sealed>]
type internal Initializer =
    static member Init(config : Configuration,
                        workerId : string, 
                        loggers : ISystemLogger seq, 
                        useAppDomainIsolation : bool,
                        maxConcurrentJobs : int, 
                        customResources : ResourceRegistry) =
        async {
            let logger = AttacheableLogger.FromLoggers(loggers)
            
            logger.LogInfof "Initializing worker %A" workerId
            let workerId = new WorkerId(workerId) :> IWorkerId

            logger.LogInfof "Creating RuntimeManager"
            let runtimeManager = RuntimeManager.CreateForWorker(config, workerId, loggers, customResources)

            logger.LogInfo "Installed resources"
            for rsc in runtimeManager.Resources.InstalledResources do
                logger.LogInfo rsc

            let jobEvaluator = 
                if useAppDomainIsolation then
                    logger.LogInfof "Initializing AppDomainpool evaluator"
                    let init () =
                        logger.LogInfof "Initializing Application Domain %A" System.AppDomain.CurrentDomain.FriendlyName
                    let managerF = DomainLocal.Create(fun () -> RuntimeManager.CreateForWorker(config, workerId, loggers, customResources) :> IRuntimeManager , workerId)
                    AppDomainJobEvaluator.Create(managerF, init) :> ICloudJobEvaluator
                else
                    new LocalJobEvaluator(runtimeManager, workerId) :> ICloudJobEvaluator

            logger.LogInfo "Creating worker agent"
            let! agent = WorkerAgent.Create(runtimeManager, workerId, jobEvaluator, maxConcurrentJobs, submitPerformanceMetrics = true)
            logger.LogInfo "Starting worker agent"
            do! agent.Start()
            logger.LogInfo "Worker agent started"
            return agent
        }
