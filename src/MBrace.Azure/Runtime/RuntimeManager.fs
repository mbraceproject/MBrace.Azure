namespace MBrace.Azure.Runtime

open System
open System.Reflection

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Components
open MBrace.Azure
open MBrace.Azure.Store

[<AutoSerializable(false)>]
type ClusterManager private (configB : Configuration, config : ClusterId, uuid : string, systemLogger : AttacheableLogger, resources : ResourceRegistry) =
    do systemLogger.LogInfof "RuntimeManager Id = %A" config.Id

    do systemLogger.LogInfo "Creating worker manager"
    let workerManager = WorkerManager.Create(config, systemLogger)
    do systemLogger.LogInfo "Creating work item manager"
    let jobManager    = WorkItemManager.Create(config, workerManager, systemLogger)
    do systemLogger.LogInfo "Creating task manager"
    let processManager   = CloudProcessManager.Create(config, systemLogger)
    do systemLogger.LogInfo "Creating assembly manager"
    let store = resources.Resolve<ICloudFileStore>()

    let assemblyManager =
        let serializer = resources.Resolve<ISerializer>()
        let ignoredAssemblies = [| Assembly.GetExecutingAssembly() |]
        let config = StoreAssemblyManagerConfiguration.Create(store, serializer, container = config.VagabondContainer, ignoredAssemblies = ignoredAssemblies, compressAssemblies = true)
        StoreAssemblyManager.Create(config, localLogger = systemLogger)

    do
        let cloudValueProvider = resources.Resolve<ICloudValueProvider>()
        let csc = ClosureSiftConfiguration.Create(cloudValueProvider, siftThreshold = 5L * 1024L * 1024L)
        let manager = ClosureSiftManager.Create(csc, localLogger = systemLogger)
        ConfigurationRegistry.Register<ClosureSiftManager>(config, manager)

    do systemLogger.LogInfo "Creating SystemLog manager"
    let systemLogManager = new TableSystemLogManager(config)
    do systemLogger.LogInfo "Creating CloudLog manager"
    let cloudLogManager = new TableCloudLogManager(config)

    let localSystemLogger = new AttacheableLoggerManager(systemLogger)

    let cancellationEntryFactory = CancellationTokenFactory.Create(config)
    let int32CounterFactory = Int32CounterFactory.Create(config)
    let resultAggregatorFactory = ResultAggregatorFactory.Create(config)

    do systemLogger.LogInfo "RuntimeManager initialization complete"

    member this.RuntimeManagerId = uuid
    member this.Resources = resources
    member this.Configuration = config
    member this.ConfigurationB = configB

    member this.ResetCluster(?deleteQueues, ?deleteRuntimeState, ?deleteLogs, ?deleteUserData, ?deleteAssemblyData, ?force, ?reactivate) = async {
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
                systemLogger.LogError exc.Message
                return! Async.Raise exc
            
        if deleteQueues then 
            systemLogger.LogWarningf "Deleting Queues %A, %A." config.RuntimeQueue config.RuntimeTable
            do! config.ClearRuntimeQueues()
            
        if deleteRuntimeState then 
            systemLogger.LogWarningf "Deleting runtime Container %A and Table %A." config.RuntimeContainer config.RuntimeTable
            do! config.ClearRuntimeState()
            
        if deleteLogs then 
            systemLogger.LogWarningf "Deleting system log Table %A." config.RuntimeLogsTable
            do! config.ClearRuntimeLogs()

        if deleteUserData then 
            systemLogger.LogWarningf "Deleting UserData Container %A and Table %A." config.UserDataContainer config.UserDataTable
            do! config.ClearUserData()

        if deleteAssemblyData then
            systemLogger.LogWarningf "Deleting Vagabond Container %A." config.VagabondContainer
            do! config.ClearVagabondData()
    
        if reactivate then        
            systemLogger.LogInfo "Reactivating configuration."
            let rec loop retryCount = async {
                systemLogger.LogInfof "RetryCount %d." retryCount
                let! step2 = Async.Catch <| config.InitializeAll()
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
        member this.Id                       = config :> _
        member this.Serializer               = ProcessConfiguration.Serializer :> _
        member this.WorkerManager            = workerManager :> _
        member this.ProcessManager           = processManager :> _
        member this.WorkItemQueue            = jobManager :> _
        member this.AssemblyManager          = assemblyManager :> _
        member this.CancellationEntryFactory = cancellationEntryFactory
        member this.CounterFactory           = int32CounterFactory
        member this.ResetClusterState()      = this.ResetCluster()
        member this.ResourceRegistry         = resources
        member this.ResultAggregatorFactory  = resultAggregatorFactory
        member this.CloudLogManager          = cloudLogManager :> _
        member this.RuntimeSystemLogManager  = systemLogManager :> _
        member this.LocalSystemLogManager    = localSystemLogger :> _


    static member private GetDefaultResources(config : ClusterId, customResources : ResourceRegistry) =
        let fileStore = BlobStore.Create(config.StorageAccount, defaultContainer = config.UserDataContainer)
        let atomProvider = TableAtomProvider.Create(config.StorageAccount, defaultTable = config.UserDataTable)
        let dictionaryProvider = TableDictionaryProvider.Create(config.StorageAccount)
        let queueProvider = ServiceBusQueueProvider.Create(config.ServiceBusAccount)

        let cloudValueProvider =
            let cloudValueStore = (fileStore :> ICloudFileStore).WithDefaultDirectory config.CloudValueContainer
            let mkCache () = ProcessConfiguration.ObjectCache
            let mkLocalCachingStore () = (ProcessConfiguration.FileStore :> ICloudFileStore).WithDefaultDirectory "cloudValueCache"
            StoreCloudValueProvider.InitCloudValueProvider(cloudValueStore, cacheFactory = mkCache, localFileStore = mkLocalCachingStore, 
                                                            shadowPersistObjects = true, compressionLevel = CompressionLevel.Optimal)

        resource {
            yield fileStore :> ICloudFileStore
            yield cloudValueProvider :> ICloudValueProvider
            yield atomProvider :> ICloudAtomProvider
            yield dictionaryProvider :> ICloudDictionaryProvider
            yield queueProvider :> ICloudQueueProvider
            yield new VagabondFsPicklerBinarySerializer() :> ISerializer
            yield! customResources
        }

    static member CreateForWorker(configB : Configuration, workerId : IWorkerId, logger : AttacheableLogger, customResources : ResourceRegistry) =
        let config = ClusterId.Activate configB
        logger.LogInfof "Activating cluster configuration: '%s'." config.Id
        logger.LogInfof "Creating resources"
        let resources = ClusterManager.GetDefaultResources(config, customResources)
        logger.LogInfof "Creating RuntimeManager for Worker %A" workerId
        let runtime = new ClusterManager(configB, config, workerId.Id, logger, resources)
        runtime.SetLocalWorkerId(workerId)
        runtime

    static member CreateForAppDomain(configB : Configuration, workerId : IWorkerId, mlogger : MarshaledLogger, customResources) =
        let config = ClusterId.Activate configB
        let logger = AttacheableLogger.Create(makeAsynchronous = true)
        let _ = logger.AttachLogger(mlogger)
        let resources = ClusterManager.GetDefaultResources(config, customResources)
        logger.LogInfof "Creating RuntimeManager for AppDomain %A" AppDomain.CurrentDomain.FriendlyName
        let runtime = new ClusterManager(configB, config, workerId.Id, logger, resources)
        runtime

    static member CreateForClient(configB : Configuration, clientId : string, logger : AttacheableLogger, customResources) =
        let config = ClusterId.Activate configB
        logger.LogInfof "Activating cluster configuration: '%s'." config.Id
        logger.LogInfof "Creating resources"
        let resources = ClusterManager.GetDefaultResources(config, customResources)
        logger.LogInfof "Creating RuntimeManager for Client %A" clientId
        let runtime = new ClusterManager(configB, config, clientId, logger, resources)
        runtime
