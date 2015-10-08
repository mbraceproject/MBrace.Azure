namespace MBrace.Azure.Runtime

open System
open System.Reflection

open Nessos.FsPickler

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Components
open MBrace.Azure
open MBrace.Azure.Store

/// The ClusterManager contains all resources necessary for running
/// MBrace.Azure cluster operations for the current process.
[<AutoSerializable(false)>]
type ClusterManager =
    {
        ClusterId               : ClusterId
        Configuration           : Configuration
        Serializer              : FsPicklerSerializer
        Logger                  : ISystemLogger
        Resources               : ResourceRegistry
        WorkerManager           : WorkerManager
        WorkQueue               : WorkItemQueue
        ProcessManager          : CloudProcessManager
        AssemblyManager         : StoreAssemblyManager
        LocalLoggerManager      : ILocalSystemLogManager
        SystemLoggerManager     : TableSystemLogManager
        CloudLoggerManager      : TableCloudLogManager
        CancellationFactory     : TableCancellationTokenFactory
        CounterFactory          : TableCounterFactory
        ResultAggregatorFactory : TableResultAggregatorFactory
    }
with
    interface IRuntimeManager with
        member r.Id                       = r.ClusterId :> _
        member r.Serializer               = r.Serializer
        member r.WorkerManager            = r.WorkerManager :> _
        member r.ProcessManager           = r.ProcessManager :> _
        member r.WorkItemQueue            = r.WorkQueue :> _
        member r.AssemblyManager          = r.AssemblyManager :> _
        member r.CancellationEntryFactory = r.CancellationFactory :> _
        member r.CounterFactory           = r.CounterFactory :> _
        member r.ResetClusterState()      = r.ResetCluster()
        member r.ResourceRegistry         = r.Resources
        member r.ResultAggregatorFactory  = r.ResultAggregatorFactory :> _
        member r.CloudLogManager          = r.CloudLoggerManager :> _
        member r.RuntimeSystemLogManager  = r.SystemLoggerManager :> _
        member r.LocalSystemLogManager    = r.LocalLoggerManager

    /// Initializes a topic maintenance agent in the local process
    member r.InitTopicMonitor() = TopicMonitor.Create(r.ClusterId, r.WorkerManager, r.Logger)

    /// Resets the cluster store state with supplied parameters
    member r.ResetCluster(?deleteQueues : bool, ?deleteRuntimeState : bool, ?deleteLogs : bool, ?deleteUserData : bool, 
                                ?deleteAssemblyData : bool, ?force : bool, ?reactivate : bool) = async {

        let deleteQueues = defaultArg deleteQueues true
        let deleteRuntimeState = defaultArg deleteRuntimeState true
        let deleteLogs = defaultArg deleteLogs true
        let deleteUserData = defaultArg deleteUserData false
        let deleteAssemblyData = defaultArg deleteAssemblyData false
        let force = defaultArg force false
        let reactivate = defaultArg reactivate true

        let clusterId = r.ClusterId
        let logger = r.Logger

        if not force then
            let! workers = (r.WorkerManager :> IWorkerManager).GetAvailableWorkers()
            if  workers.Length > 0 then
                let exc = InvalidOperationException(sprintf "Found %d active workers. Shutdown workers first or 'force' reset." workers.Length)
                logger.LogError exc.Message
                return! Async.Raise exc
            
        if deleteQueues then 
            logger.LogWarningf "Deleting Queues %A, %A." clusterId.RuntimeQueue clusterId.RuntimeTable
            do! clusterId.ClearRuntimeQueues()
            
        if deleteRuntimeState then 
            logger.LogWarningf "Deleting runtime Container %A and Table %A." clusterId.RuntimeContainer clusterId.RuntimeTable
            do! clusterId.ClearRuntimeState()
            
        if deleteLogs then 
            logger.LogWarningf "Deleting system log Table %A." clusterId.RuntimeLogsTable
            do! clusterId.ClearRuntimeLogs()

        if deleteUserData then 
            logger.LogWarningf "Deleting UserData Container %A and Table %A." clusterId.UserDataContainer clusterId.UserDataTable
            do! clusterId.ClearUserData()

        if deleteAssemblyData then
            logger.LogWarningf "Deleting Vagabond Container %A." clusterId.VagabondContainer
            do! clusterId.ClearVagabondData()
    
        if reactivate then        
            logger.LogInfo "Reactivating configuration."
            do! clusterId.InitializeAllStoreResources()

        logger.LogInfo "Reset : done."
        return ()
    }

    /// <summary>
    ///     Initializes a runtime manager object for the current process with provided parameters.
    /// </summary>
    /// <param name="configuration">Cluster store configuration object.</param>
    /// <param name="customResources">User-supplied custom resources for the cluster.</param>
    /// <param name="systemLogger">System logger used by the local process.</param>
    static member Create(configuration : Configuration, ?customResources : ResourceRegistry, ?systemLogger : ISystemLogger) = async {
        let configuration = FsPickler.Clone configuration // isolate external mutations to configuration object

        let logger = AttacheableLogger.Create(makeAsynchronous = false)
        match systemLogger with Some l -> logger.AttachLogger l |> ignore | None -> ()

        let clusterId = ClusterId.Activate configuration
        logger.LogInfof "Activating cluster configuration:\n\tStorage: %s\n\tServiceBus: %s\n\tHashcode: %s" configuration.StorageAccount configuration.ServiceBusAccount clusterId.Hash

        logger.LogInfof "Initializing Azure store entities"
        do! clusterId.InitializeAllStoreResources(maxRetries = 20, retryInterval = 3000)

        logger.LogInfof "Creating MBrace storage primitives"
        let fileStore = BlobStore.Create(clusterId.StorageAccount, defaultContainer = clusterId.UserDataContainer)
        let atomProvider = TableAtomProvider.Create(clusterId.StorageAccount, defaultTable = clusterId.UserDataTable)
        let dictionaryProvider = TableDictionaryProvider.Create(clusterId.StorageAccount)
        let queueProvider = ServiceBusQueueProvider.Create(clusterId.ServiceBusAccount)
        let serializer = VagabondFsPicklerBinarySerializer()

        let cloudValueProvider =
            let cloudValueStore = (fileStore :> ICloudFileStore).WithDefaultDirectory clusterId.CloudValueContainer
            let mkCache () = ProcessConfiguration.ObjectCache
            let mkLocalCachingStore () = (ProcessConfiguration.FileStore :> ICloudFileStore).WithDefaultDirectory "cloudValueCache"
            let provider = StoreCloudValueProvider.InitCloudValueProvider(cloudValueStore, cacheFactory = mkCache, localFileStore = mkLocalCachingStore, 
                                                            shadowPersistObjects = true, compressionLevel = CompressionLevel.Optimal)
            provider.InstallCacheOnLocalAppDomain()
            provider

        let resources = resource {
            match customResources with Some r -> yield! r | None -> ()
            yield fileStore :> ICloudFileStore
            yield cloudValueProvider :> ICloudValueProvider
            yield atomProvider :> ICloudAtomProvider
            yield dictionaryProvider :> ICloudDictionaryProvider
            yield queueProvider :> ICloudQueueProvider
            yield serializer :> ISerializer
        }

        logger.LogInfo "Creating worker manager"
        let workerManager = WorkerManager.Create(clusterId, logger)
        logger.LogInfo "Creating work item manager"
        let! workManager   = WorkItemQueue.Create(clusterId, logger)
        logger.LogInfo "Creating task manager"
        let processManager   = CloudProcessManager.Create(clusterId, logger)
        logger.LogInfo "Creating assembly manager"
        let assemblyManager =
            let ignoredAssemblies = [| Assembly.GetExecutingAssembly() |]
            let config = StoreAssemblyManagerConfiguration.Create(fileStore, serializer, container = clusterId.VagabondContainer, ignoredAssemblies = ignoredAssemblies, compressAssemblies = true)
            StoreAssemblyManager.Create(config, localLogger = logger)

        logger.LogInfof "Creating closure sift manager."
        let siftManager =
            let csc = ClosureSiftConfiguration.Create(cloudValueProvider, siftThreshold = 5L * 1024L * 1024L)
            ClosureSiftManager.Create(csc, localLogger = logger)
        ConfigurationRegistry.Register<ClosureSiftManager>(clusterId, siftManager)

        logger.LogInfo "Creating SystemLog manager"
        let systemLogManager = new TableSystemLogManager(clusterId)
        logger.LogInfo "Creating CloudLog manager"
        let cloudLogManager = new TableCloudLogManager(clusterId)

        logger.LogInfo "Initializing synchronization primitives."
        let cancellationEntryFactory = TableCancellationTokenFactory.Create(clusterId)
        let counterFactory = TableCounterFactory.Create(clusterId)
        let resultAggregatorFactory = TableResultAggregatorFactory.Create(clusterId)

        logger.LogInfo "RuntimeManager initialization complete"

        return {
            ClusterId = clusterId
            Configuration = configuration
            Serializer = ProcessConfiguration.BinarySerializer :> FsPicklerSerializer
            Logger = logger
            Resources = resources
            WorkerManager = workerManager
            WorkQueue = workManager
            ProcessManager = processManager
            AssemblyManager = assemblyManager
            LocalLoggerManager = new AttacheableLoggerManager(logger) :> ILocalSystemLogManager
            SystemLoggerManager = systemLogManager
            CloudLoggerManager = cloudLogManager
            CancellationFactory = cancellationEntryFactory
            CounterFactory = counterFactory
            ResultAggregatorFactory = resultAggregatorFactory
        }
    }