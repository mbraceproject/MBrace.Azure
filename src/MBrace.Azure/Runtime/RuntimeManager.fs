namespace MBrace.Azure.Runtime

open System

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Components
open MBrace.Azure
open MBrace.Azure.Store

[<AutoSerializable(false)>]
type ClusterManager private (config : Configuration, uuid : string, systemLogger : AttacheableLogger, resources : ResourceRegistry) =
    let configId = config.GetConfigurationId()
    do systemLogger.LogInfof "RuntimeManager Id = %A" (configId :> IRuntimeId).Id

    do systemLogger.LogInfo "Creating worker manager"
    let workerManager = WorkerManager.Create(configId, systemLogger)
    do systemLogger.LogInfo "Creating job manager"
    let jobManager    = JobManager.Create(configId, systemLogger)
    do systemLogger.LogInfo "Creating task manager"
    let taskManager   = TaskManager.Create(configId, systemLogger)
    do systemLogger.LogInfo "Creating assembly manager"
    let store = resources.Resolve<ICloudFileStore>()

    let assemblyManager =
        let serializer = resources.Resolve<ISerializer>()
        let config = StoreAssemblyManagerConfiguration.Create(store, serializer, container = configId.VagabondContainer)
        StoreAssemblyManager.Create(config, localLogger = systemLogger)

    do
        let cloudValueProvider = resources.Resolve<ICloudValueProvider>()
        let csc = ClosureSiftConfiguration.Create(cloudValueProvider, siftThreshold = 5L * 1024L * 1024L)
        let manager = ClosureSiftManager.Create(csc, localLogger = systemLogger)
        ConfigurationRegistry.Register<ClosureSiftManager>(configId, manager)

    do systemLogger.LogInfo "Creating CloudLog manager"
    let cloudLogManager = CloudLogManager.Create(configId)

    let cancellationEntryFactory = CancellationTokenFactory.Create(configId)
    let int32CounterFactory = Int32CounterFactory.Create(configId)
    let resultAggregatorFactory = ResultAggregatorFactory.Create(configId)

    do systemLogger.LogInfo "RuntimeManager initialization complete"

    member this.RuntimeManagerId = uuid
    member this.Resources = resources
    member this.ConfigurationId = configId
    member this.Configuration = config

    member this.ResetCluster(deleteQueues, deleteState, deleteLogs, deleteUserData, deleteVagabondData, force, reactivate) =
        async {
            if not force then
                let! workers = (workerManager :> IWorkerManager).GetAvailableWorkers()
                if  workers.Length > 0 then
                    let exc = RuntimeException(sprintf "Found %d active workers. Shutdown workers first or 'force' reset." workers.Length)
                    systemLogger.LogError exc.Message
                    return! Async.Raise exc
             
            
            if deleteQueues then 
                systemLogger.LogWarningf "Deleting Queues %A, %A." configId.RuntimeQueue configId.RuntimeTable
                do! Config.DeleteRuntimeQueues(configId)
            
            if deleteState then 
                systemLogger.LogWarningf "Deleting Container %A and Table %A." configId.RuntimeContainer configId.RuntimeTable
                do! Config.DeleteRuntimeState(configId)
            
            if deleteLogs then 
                systemLogger.LogWarningf "Deleting Logs Table %A." configId.RuntimeLogsTable
                do! Config.DeleteRuntimeLogs(configId)

            if deleteUserData then 
                systemLogger.LogWarningf "Deleting UserData Container %A and Table %A." configId.UserDataContainer configId.UserDataTable
                do! Config.DeleteUserData(configId)

            if deleteVagabondData then
                systemLogger.LogWarningf "Deleting Vagadbond Container %A." configId.VagabondContainer
                do! Config.DeleteVagabondData(configId)
    
            if reactivate then        
                systemLogger.LogInfo "Reactivating configuration."
                let rec loop retryCount = async {
                    systemLogger.LogInfof "RetryCount %d." retryCount
                    let! step2 = Async.Catch <| Config.ReactivateAsync(configId)
                    match step2 with
                    | Choice1Of2 _ -> ()
                    | Choice2Of2 ex ->
                        systemLogger.LogWarningf "Failed with %A\nWaiting." ex
                        do! Async.Sleep 10000
                        return! loop (retryCount + 1)
                }
                do! loop 0
            systemLogger.LogInfo "Reset : done."
            return ()
        }

    member private this.SetLocalWorkerId(workerId : IWorkerId) =
        jobManager.SetLocalWorkerId(workerId)

    interface IRuntimeManager with
        member this.Id                       = configId :> _
        member this.Serializer               = Config.Pickler :> _
        member this.WorkerManager            = workerManager :> _
        member this.TaskManager              = taskManager :> _
        member this.JobQueue                 = jobManager :> _
        member this.AssemblyManager          = assemblyManager :> _
        member this.SystemLogger             = systemLogger :> _
        member this.AttachSystemLogger l     = systemLogger.AttachLogger l
        member this.CancellationEntryFactory = cancellationEntryFactory
        member this.CounterFactory           = int32CounterFactory
        member this.ResetClusterState()      = this.ResetCluster(true, true, true, false, false, false, true)
        member this.ResourceRegistry         = resources
        member this.ResultAggregatorFactory  = resultAggregatorFactory
        member this.CloudLogManager          = cloudLogManager :> _
        member this.LogLevel                 = systemLogger.LogLevel
        member this.LogLevel with set l      = systemLogger.LogLevel <- l


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

    static member CreateForWorker(config : Configuration, workerId : IWorkerId, logger : AttacheableLogger, customResources) =
        logger.LogInfof "Activating configuration with Id %A" config.Id
        Config.Activate(config, true)
        logger.LogInfof "Creating resources"
        let resources = ClusterManager.GetDefaultResources(config, customResources)
        logger.LogInfof "Creating RuntimeManager for Worker %A" workerId
        let runtime = new ClusterManager(config, workerId.Id, logger, resources)
        runtime.SetLocalWorkerId(workerId)
        runtime

    static member CreateForAppDomain(config : Configuration, workerId : IWorkerId, mlogger : MarshaledLogger, customResources) =
        let logger = AttacheableLogger.Create(makeAsynchronous = true)
        let _ = logger.AttachLogger(mlogger)
        logger.LogInfof "Activating configuration with Id %A" config.Id
        Config.Activate(config, false)
        logger.LogInfof "Creating resources"
        let resources = ClusterManager.GetDefaultResources(config, customResources)
        logger.LogInfof "Creating RuntimeManager for AppDomain %A" AppDomain.CurrentDomain.FriendlyName
        let runtime = new ClusterManager(config, workerId.Id, logger, resources)
        runtime

    static member CreateForClient(config : Configuration, clientId : string, logger : AttacheableLogger, customResources) =
        logger.LogInfof "Activating configuration with Id %A" config.Id
        Config.Activate(config, true)
        logger.LogInfof "Creating resources"        
        let resources = ClusterManager.GetDefaultResources(config, customResources)
        logger.LogInfof "Creating RuntimeManager for Client %A" clientId
        let runtime = new ClusterManager(config, clientId, logger, resources)
        runtime
