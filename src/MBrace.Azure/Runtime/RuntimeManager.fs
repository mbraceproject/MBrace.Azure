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
        let config = StoreAssemblyManagerConfiguration.Create(store, serializer, container = config.VagabondContainer)
        StoreAssemblyManager.Create(config, localLogger = logger)

    do
        let cloudValueProvider = resources.Resolve<ICloudValueProvider>()
        let csc = ClosureSiftConfiguration.Create(cloudValueProvider, siftThreshold = 5L * 1024L * 1024L)
        let manager = ClosureSiftManager.Create(csc, localLogger = logger)
        ConfigurationRegistry.Register<ClosureSiftManager>(config, manager)

    do logger.LogInfo "Creating CloudLog manager"
    let cloudLogManager = CloudLogManager.Create(config)

    let cancellationEntryFactory = CancellationTokenFactory.Create(config)
    let int32CounterFactory = Int32CounterFactory.Create(config)
    let resultAggregatorFactory = ResultAggregatorFactory.Create(config)

    do logger.LogInfo "RuntimeManager initialization complete"

    member this.RuntimeManagerId = uuid
    member this.Resources = resources
    member this.ConfigurationId = config

    member this.ResetCluster(deleteQueues, deleteState, deleteLogs, deleteUserData, deleteVagabondData, force, reactivate) =
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

            if deleteVagabondData then
                logger.LogWarningf "Deleting Vagadbond Container %A." config.VagabondContainer
                do! Config.DeleteVagabondData(config)
    
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
        member this.ResetClusterState()      = this.ResetCluster(true, true, true, false, false, false, true)
        member this.ResourceRegistry         = resources
        member this.ResultAggregatorFactory  = resultAggregatorFactory
        member this.CloudLogManager          = cloudLogManager :> _
        member this.LogLevel                 = logger.LogLevel
        member this.LogLevel with set l      = logger.LogLevel <- l


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
            let cloudValueStore = (fileStore :> ICloudFileStore).WithDefaultDirectory config.CloudValueContainer
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
