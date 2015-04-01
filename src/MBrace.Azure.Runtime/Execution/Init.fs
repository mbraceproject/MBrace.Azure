namespace MBrace.Azure.Runtime

open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Info
open MBrace.Azure.Runtime.Utilities
open MBrace.Azure.Runtime.Primitives
open MBrace.Runtime
open System.Diagnostics
open MBrace
open MBrace.Continuation
open MBrace.Store
open System

/// Common initialization for Service and AppDomains.
type Init =
    static member Initializer ( configuration : Configuration,
                                serviceId : string, 
                                isDefaultInitialization : bool, 
                                customLogger : ICloudLogger,
                                ignoreVersion : bool,
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
            then logf "%s" <| ReleaseInfo.signatureString()

            logf "Initializing RuntimeState"
            let! state = RuntimeState.FromConfiguration(config, ignoreVersionCompatibility = ignoreVersion)
            state.Logger.Attach(logger)
            
            if not isDefaultInitialization
            then state.Logger.ShowAppDomainAsPrefix <- true
                 logger.ShowAppDomainAsPrefix <- false
            
            logf "Registering Subscription %s" serviceId
            state.JobQueue.Affinity <- serviceId

            if isDefaultInitialization then 
                logf "Declaring worker"
                do! state.WorkerManager.RegisterCurrent(serviceId, ?maxJobs = maxJobs)
                let record = state.WorkerManager.Current
                logf "Declared worker : %s \nPID : %d \nServiceId : %s" record.Hostname record.ProcessId.Value record.Id
                Async.Start(state.WorkerManager.HeartbeatLoop(TimeSpan.FromSeconds(2.)))
                logf "Started heartbeat loop" 
            else
                do! state.WorkerManager.RegisterLocal(serviceId)

            let resources = resource { 
                yield Configuration.Serializer
                //yield logger // TODO : is this needed?
                yield state.WorkerManager
                yield state.ProcessManager 
            }

            return state, resources
        }
        
