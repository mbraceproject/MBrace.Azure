namespace MBrace.Azure.Runtime

open System

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Components
open MBrace.Azure
open MBrace.Azure.Store

[<AutoSerializable(false)>]
type RuntimeManager private (config : ConfigurationId, uuid : string, customLogger : ISystemLogger, resources : ResourceRegistry) =
    let logger = AttacheableLogger.Create(makeAsynchronous = true)
    do ignore <| logger.AttachLogger customLogger
    do logger.LogInfof "RuntimeManager Id = %A" (config :> IRuntimeId).Id

    do logger.LogInfo "Creating worker manager"
    let workerManager = WorkerManager.Create(config, logger)
    do logger.LogInfo "Creating job manager"
    let jobManager    = JobManager.Create(config, logger)
    do logger.LogInfo "Creating task manager"
    let taskManager   = TaskManager.Create(config, logger)
    do logger.LogInfo "Creating assembly manager"
    let store = resources.Resolve<ICloudFileStore>()

    let assemblyManager =
        let serializer = resources.Resolve<ISerializer>()
        let config = StoreAssemblyManagerConfiguration.Create(store, serializer, container = "vagabond")
        StoreAssemblyManager.Create(config)

    // This implementation will currently create a separate log container per task; need to fix this.
    let taskLogger = StoreCloudLogManager.Create(store, new DefaultStoreLogSchema(store), sysLogger = logger)

    let cancellationEntryFactory = CancellationTokenFactory.Create(config)
    let int32CounterFactory = Int32CounterFactory.Create(config)
    let resultAggregatorFactory = ResultAggregatorFactory.Create(config)

    member this.RuntimeManagerId = uuid
    member this.Resources = resources
    member this.ConfigurationId = config

    member this.ResetCluster(deleteQueues, deleteState, deleteLogs, deleteUserData, force, reactivate) =
        async {
            if not force then
                let! workers = (workerManager :> IWorkerManager).GetAvailableWorkers()
                if  workers.Length > 0 then
                    let exc = RuntimeException(sprintf "Found %d active workers. Shutdown workers first or 'force' reset." workers.Length)
                    logger.LogError exc.Message
                    return! Async.Raise exc
             
            
            if deleteQueues then 
                logger.LogWarningf "Deleting Queues %A, %A." config.RuntimeQueue config.RuntimeTable
                do! Config.DeleteRuntimeQueues(config)
            
            if deleteState then 
                logger.LogWarningf "Deleting Container %A and Table %A." config.RuntimeContainer config.RuntimeTable
                do! Config.DeleteRuntimeState(config)
            
            if deleteLogs then 
                logger.LogWarningf "Deleting Logs Table %A." config.RuntimeLogsTable
                do! Config.DeleteRuntimeLogs(config)

            if deleteUserData then 
                logger.LogWarningf "Deleting UserData Container %A and Table %A." config.UserDataContainer config.UserDataTable
                do! Config.DeleteUserData(config)
    
            if reactivate then        
                logger.LogInfo "Reactivating configuration."
                let rec loop retryCount = async {
                    logger.LogInfof "RetryCount %d." retryCount
                    let! step2 = Async.Catch <| Config.ReactivateAsync(config)
                    match step2 with
                    | Choice1Of2 _ -> ()
                    | Choice2Of2 ex ->
                        logger.LogWarningf "Failed with %A\nWaiting." ex
                        do! Async.Sleep 10000
                        return! loop (retryCount + 1)
                }
                do! loop 0
            logger.LogInfo "Reset : done."
            return ()
        }

    member private this.SetLocalWorkerId(workerId : IWorkerId) =
        jobManager.SetLocalWorkerId(workerId)

    interface IRuntimeManager with
        member this.Id                       = config :> _
        member this.Serializer               = Config.Pickler :> _
        member this.WorkerManager            = workerManager :> _
        member this.TaskManager              = taskManager :> _
        member this.JobQueue                 = jobManager :> _
        member this.AssemblyManager          = assemblyManager :> _
        member this.SystemLogger             = logger :> _
        member this.AttachSystemLogger l     = logger.AttachLogger l
        member this.CancellationEntryFactory = cancellationEntryFactory
        member this.CounterFactory           = int32CounterFactory
        member this.ResetClusterState()      = this.ResetCluster(true, true, true, false, false, true)
        member this.ResourceRegistry         = resources
        member this.ResultAggregatorFactory  = resultAggregatorFactory
        member this.CloudLogManager          = taskLogger :> _
        member this.LogLevel                 = logger.LogLevel
        member this.LogLevel with set l      = logger.LogLevel <- l
//        member this.GetCloudLogger(worker : IWorkerId, job : CloudJob) = 
//            let cloudLogger = CloudStorageLogger(config, worker, job.TaskEntry.Id)
//            let consoleLogger = new ConsoleLogger(showDate = true)
//            
//            { new ICloudLogger with
//                  member x.Log(entry : string) : unit = 
//                      consoleLogger.Log LogLevel.None entry
//                      (cloudLogger :> ICloudLogger).Log(entry) }

    static member private GetDefaultResources(config : Configuration, customResources : ResourceRegistry) =
        let storeConn = config.StorageConnectionString
        let sbusConn = config.ServiceBusConnectionString
        let config = config.GetConfigurationId()
        let fileStore = BlobStore.Create(storeConn, defaultContainer = config.UserDataContainer)
        let atomProvider = AtomProvider.Create(storeConn, defaultTable = config.UserDataTable)
        let dictionaryProvider = CloudDictionaryProvider.Create(storeConn)
        let queueProvider = QueueProvider.Create(sbusConn)

        // TODO : specify Vagabond and CloudValue containers in Configuration object

        let cloudValueProvider =
            let cloudValueStore = (fileStore :> ICloudFileStore).WithDefaultDirectory "cloudvalue"
            let mkCache () = Config.ObjectCache
            let mkLocalCachingStore () = (Config.FileStore :> ICloudFileStore).WithDefaultDirectory "cloudValueCache"
            StoreCloudValueProvider.InitCloudValueProvider(cloudValueStore, cacheFactory = mkCache, localFileStore = mkLocalCachingStore, shadowPersistObjects = true)

        resource {
            yield fileStore :> ICloudFileStore
            yield cloudValueProvider :> ICloudValueProvider
            yield atomProvider :> ICloudAtomProvider
            yield dictionaryProvider :> ICloudDictionaryProvider
            yield queueProvider :> ICloudQueueProvider
            yield Config.Serializer
            yield! customResources
        }

    static member CreateForWorker(config : Configuration, workerId : IWorkerId, customLogger : ISystemLogger, customResources) =
        customLogger.LogInfof "Activating configuration with Id %A" config.Id
        Config.Activate(config, true)
        customLogger.LogInfof "Creating resources"
        let resources = RuntimeManager.GetDefaultResources(config, customResources)
        customLogger.LogInfof "Creating RuntimeManager for Worker %A" workerId
        let runtime = new RuntimeManager(config.GetConfigurationId(), workerId.Id, customLogger, resources)
        runtime.SetLocalWorkerId(workerId)
        runtime

    static member CreateForAppDomain(config : Configuration, workerId : IWorkerId, customLogger : ISystemLogger, customResources) =
        customLogger.LogInfof "Activating configuration with Id %A" config.Id
        Config.Activate(config, false)
        customLogger.LogInfof "Creating resources"
        let resources = RuntimeManager.GetDefaultResources(config, customResources)
        customLogger.LogInfof "Creating RuntimeManager for AppDomain %A" AppDomain.CurrentDomain.FriendlyName
        let runtime = new RuntimeManager(config.GetConfigurationId(), workerId.Id, customLogger, resources)
        runtime

    static member CreateForClient(config : Configuration, clientId : string, customLogger : ISystemLogger, customResources) =
        customLogger.LogInfof "Activating configuration with Id %A" config.Id
        Config.Activate(config, true)
        customLogger.LogInfof "Creating resources"        
        let resources = RuntimeManager.GetDefaultResources(config, customResources)
        customLogger.LogInfof "Creating RuntimeManager for Client %A" clientId
        let runtime = new RuntimeManager(config.GetConfigurationId(), clientId, customLogger, resources)
        runtime
