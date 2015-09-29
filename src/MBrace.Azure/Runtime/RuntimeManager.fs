namespace MBrace.Azure.Runtime

open System
open System.Reflection

open Nessos.FsPickler

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Components
open MBrace.Azure
open MBrace.Azure.Store

[<AutoSerializable(false)>]
type ClusterManager private (configB : Configuration, config : ClusterId, logger : AttacheableLogger, resources : ResourceRegistry) =
    do logger.LogInfof "RuntimeManager Id = %A" config.Id

    do logger.LogInfo "Creating worker manager"
    let workerManager = WorkerManager.Create(config, logger)
    do logger.LogInfo "Creating work item manager"
    let workManager    = WorkItemManager.Create(config, workerManager, logger)
    do logger.LogInfo "Creating task manager"
    let processManager   = CloudProcessManager.Create(config, logger)
    do logger.LogInfo "Creating assembly manager"
    let fileStore = resources.Resolve<ICloudFileStore>()
    let serializer = resources.Resolve<ISerializer>()
    let assemblyManager =
        let ignoredAssemblies = [| Assembly.GetExecutingAssembly() |]
        let config = StoreAssemblyManagerConfiguration.Create(fileStore, serializer, container = config.VagabondContainer, ignoredAssemblies = ignoredAssemblies, compressAssemblies = true)
        StoreAssemblyManager.Create(config, localLogger = logger)

    do logger.LogInfo "Creating SystemLog manager"
    let systemLogManager = new TableSystemLogManager(config)
    do logger.LogInfo "Creating CloudLog manager"
    let cloudLogManager = new TableCloudLogManager(config)

    let loggerManager = new AttacheableLoggerManager(logger)

    let cancellationEntryFactory = CancellationTokenFactory.Create(config)
    let int32CounterFactory = Int32CounterFactory.Create(config)
    let resultAggregatorFactory = ResultAggregatorFactory.Create(config)

    do logger.LogInfo "RuntimeManager initialization complete"

    member this.Resources = resources
    member this.ClusterId = config
    member this.Configuration = configB
    member this.WorkerManager = workerManager
    member this.WorkItemManager = workManager
    member this.SystemLogManager = systemLogManager
    member this.CloudLogManager = cloudLogManager
    member this.LocalLogManager = loggerManager :> ILocalSystemLogManager

    member this.ResetCluster(?deleteQueues : bool, ?deleteRuntimeState : bool, ?deleteLogs : bool, ?deleteUserData : bool, 
                                ?deleteAssemblyData : bool, ?force : bool, ?reactivate : bool) = async {

        let deleteQueues = defaultArg deleteQueues true
        let deleteRuntimeState = defaultArg deleteRuntimeState true
        let deleteLogs = defaultArg deleteLogs true
        let deleteUserData = defaultArg deleteUserData false
        let deleteAssemblyData = defaultArg deleteAssemblyData false
        let force = defaultArg force false
        let reactivate = defaultArg reactivate true

        if not force then
            let! workers = (workerManager :> IWorkerManager).GetAvailableWorkers()
            if  workers.Length > 0 then
                let exc = InvalidOperationException(sprintf "Found %d active workers. Shutdown workers first or 'force' reset." workers.Length)
                logger.LogError exc.Message
                return! Async.Raise exc
            
        if deleteQueues then 
            logger.LogWarningf "Deleting Queues %A, %A." config.RuntimeQueue config.RuntimeTable
            do! config.ClearRuntimeQueues()
            
        if deleteRuntimeState then 
            logger.LogWarningf "Deleting runtime Container %A and Table %A." config.RuntimeContainer config.RuntimeTable
            do! config.ClearRuntimeState()
            
        if deleteLogs then 
            logger.LogWarningf "Deleting system log Table %A." config.RuntimeLogsTable
            do! config.ClearRuntimeLogs()

        if deleteUserData then 
            logger.LogWarningf "Deleting UserData Container %A and Table %A." config.UserDataContainer config.UserDataTable
            do! config.ClearUserData()

        if deleteAssemblyData then
            logger.LogWarningf "Deleting Vagabond Container %A." config.VagabondContainer
            do! config.ClearVagabondData()
    
        if reactivate then        
            logger.LogInfo "Reactivating configuration."
            do! config.InitializeAllStoreResources()

        logger.LogInfo "Reset : done."
        return ()
    }

    interface IRuntimeManager with
        member this.Id                       = config :> _
        member this.Serializer               = ProcessConfiguration.Serializer :> _
        member this.WorkerManager            = workerManager :> _
        member this.ProcessManager           = processManager :> _
        member this.WorkItemQueue            = workManager :> _
        member this.AssemblyManager          = assemblyManager :> _
        member this.CancellationEntryFactory = cancellationEntryFactory
        member this.CounterFactory           = int32CounterFactory
        member this.ResetClusterState()      = this.ResetCluster()
        member this.ResourceRegistry         = resources
        member this.ResultAggregatorFactory  = resultAggregatorFactory
        member this.CloudLogManager          = cloudLogManager :> _
        member this.RuntimeSystemLogManager  = systemLogManager :> _
        member this.LocalSystemLogManager    = loggerManager :> _

    static member Create(configB : Configuration, ?customResources : ResourceRegistry, ?systemLogger : ISystemLogger) = async {
        let logger = AttacheableLogger.Create(makeAsynchronous = false)
        match systemLogger with Some l -> logger.AttachLogger l |> ignore | None -> ()

        let configB = FsPickler.Clone configB

        logger.LogInfof "Activating cluster configuration: 'Storage:%s, ServiceBus:%s'." configB.StorageAccount configB.ServiceBusAccount
        let config = ClusterId.Activate configB

        logger.LogInfof "Initializing Azure store entities"
        do! config.InitializeAllStoreResources(maxRetries = 20, retryInterval = 3000)

        logger.LogInfof "Creating MBrace resource registry"
        let fileStore = BlobStore.Create(config.StorageAccount, defaultContainer = config.UserDataContainer)
        let atomProvider = TableAtomProvider.Create(config.StorageAccount, defaultTable = config.UserDataTable)
        let dictionaryProvider = TableDictionaryProvider.Create(config.StorageAccount)
        let queueProvider = ServiceBusQueueProvider.Create(config.ServiceBusAccount)
        let serializer = VagabondFsPicklerBinarySerializer()

        let cloudValueProvider =
            let cloudValueStore = (fileStore :> ICloudFileStore).WithDefaultDirectory config.CloudValueContainer
            let mkCache () = ProcessConfiguration.ObjectCache
            let mkLocalCachingStore () = (ProcessConfiguration.FileStore :> ICloudFileStore).WithDefaultDirectory "cloudValueCache"
            let provider = StoreCloudValueProvider.InitCloudValueProvider(cloudValueStore, cacheFactory = mkCache, localFileStore = mkLocalCachingStore, 
                                                            shadowPersistObjects = true, compressionLevel = CompressionLevel.Optimal)
            provider.InstallCacheOnLocalAppDomain()
            provider

        do
            let csc = ClosureSiftConfiguration.Create(cloudValueProvider, siftThreshold = 5L * 1024L * 1024L)
            let manager = ClosureSiftManager.Create(csc, localLogger = logger)
            ConfigurationRegistry.Register<ClosureSiftManager>(config, manager)

        let resources = resource {
            match customResources with Some r -> yield! r | None -> ()
            yield fileStore :> ICloudFileStore
            yield cloudValueProvider :> ICloudValueProvider
            yield atomProvider :> ICloudAtomProvider
            yield dictionaryProvider :> ICloudDictionaryProvider
            yield queueProvider :> ICloudQueueProvider
            yield serializer :> ISerializer
        }

        return new ClusterManager(configB, config, logger, resources)
    }

//    static member CreateForAppDomain(configB : Configuration, workerId : IWorkerId, mlogger : MarshaledLogger, customResources) = async {
//        let configB = FsPickler.Clone configB
//        let config = ClusterId.Activate configB
//        let logger = AttacheableLogger.Create(makeAsynchronous = true)
//        let _ = logger.AttachLogger(mlogger)
//        let resources = ClusterManager.GetDefaultResources(config, customResources)
//        logger.LogInfof "Creating RuntimeManager for AppDomain %A" AppDomain.CurrentDomain.FriendlyName
//        let runtime = new ClusterManager(configB, config, workerId.Id, logger, resources)
//        return runtime
//    }
//
//    static member CreateForClient(configB : Configuration, clientId : string, logger : AttacheableLogger, customResources) = async {
//        let configB = FsPickler.Clone configB
//        let config = ClusterId.Activate configB
//        logger.LogInfof "Activating cluster configuration: '%s'." config.Id
//        logger.LogInfof "Creating resources"
//        let resources = ClusterManager.GetDefaultResources(config, customResources)
//        logger.LogInfof "Creating RuntimeManager for Client %A" clientId
//        let runtime = new ClusterManager(configB, config, clientId, logger, resources)
//        return runtime
//    }
