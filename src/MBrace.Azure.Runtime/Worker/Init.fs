namespace MBrace.Azure.Runtime

open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common
open MBrace.Runtime
open System.Diagnostics
open MBrace
open MBrace.Continuation
open MBrace.Store
open System

module private ReleaseInfo =
    open System.Reflection

    let prettyPrint () =
        let asm = Assembly.GetExecutingAssembly()
        let attributes = 
            asm.GetCustomAttributes<AssemblyMetadataAttribute>()
            |> Seq.map (fun ma -> ma.Key, ma.Value)
            |> Map.ofSeq
        attributes.["Release Signature"]

/// Common initialization for Service and AppDomains.
type Init =
    static member Initializer ( configuration : Configuration,
                                serviceId : string, 
                                isDefaultInitialization : bool, 
                                customLogger : ICloudLogger,
                                ?maxJobs : int) =
        async {
            let logger = new LoggerCombiner(Seq.singleton customLogger, not isDefaultInitialization)
            let inline logf fmt = logger.Logf fmt

            if isDefaultInitialization 
            then logf "Starting Service %s" serviceId
            else logf "Starting AppDomain Initializer for %s" AppDomain.CurrentDomain.FriendlyName
            
            let config = configuration.WithAppendedId
            
            logf "Activating Configuration %05d, Hash %d" config.Id (hash config.ConfigurationId)
            Configuration.AddIgnoredAssembly(typeof<Init>.Assembly)
            do! Configuration.ActivateAsync(config)

            logf "Creating storage logger"
            let storageLogger = new StorageLogger(config.ConfigurationId, Worker(id = serviceId))
            logger.Attach(storageLogger)
                
            if isDefaultInitialization 
            then logf "%s" <| ReleaseInfo.prettyPrint()

            logf "Initializing RuntimeState"
            let! state = RuntimeState.FromConfiguration(config)
            state.Logger.Attach(logger)
            
            if not isDefaultInitialization
            then state.Logger.ShowAppDomainAsPrefix <- true
                 logger.ShowAppDomainAsPrefix <- false
            
            logf "Registering Subscription %s" serviceId
            state.JobQueue.Affinity <- serviceId

            let workerManager = WorkerManager.Create(config.ConfigurationId, state.Logger)
            if isDefaultInitialization then 
                logf "Declaring worker"
                let! record = workerManager.RegisterCurrent(serviceId, ?maxJobs = maxJobs)
                logf "Declared worker : %s \nPID : %d \nServiceId : %s" record.Hostname record.ProcessId.Value record.Id
                Async.Start(workerManager.HeartbeatLoop())
                logf "Started heartbeat loop" 
            else
                do! workerManager.RegisterLocal(serviceId)

            let resources = resource { 
                yield Configuration.Serializer
                yield logger
                yield workerManager
                yield state.ProcessManager 
            }

            return state, resources
        }
        
