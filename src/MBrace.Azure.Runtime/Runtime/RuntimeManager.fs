namespace MBrace.Azure.Runtime

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Azure
open System
open MBrace.Store.Internals
open MBrace.Azure.Store

[<AutoSerializable(false)>]
type RuntimeManager private (config : ConfigurationId, uuid : string, logger : ISystemLogger, resources : ResourceRegistry) =
    do logger.LogInfof "RuntimeManager Id = %A" (config :> IRuntimeId).Id

    do logger.LogInfo "Creating worker manager"
    let workerManager = WorkerManager.Create(config, logger)
    do logger.LogInfo "Creating job manager"
    let jobManager    = JobManager.Create(config, logger)
    do logger.LogInfo "Creating task manager"
    let taskManager   = TaskManager.Create(config, logger)
    do logger.LogInfo "Creating assembly manager"
    let assemblyManager =
        let store = resources.Resolve<CloudFileStoreConfiguration>()
        let serializer = resources.Resolve<ISerializer>()
        StoreAssemblyManager.Create(store, serializer, "vagabond", logger)

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
        member this.SystemLogger             = logger
        member this.CancellationEntryFactory = cancellationEntryFactory
        member this.CounterFactory           = int32CounterFactory
        member this.ResetClusterState()      = this.ResetCluster(true, true, true, false, false, true)
        member this.ResourceRegistry         = resources
        member this.ResultAggregatorFactory  = resultAggregatorFactory
        member this.GetCloudLogger(worker : IWorkerId, job : CloudJob) = 
            let cloudLogger = CloudStorageLogger(config, worker, job.TaskEntry.Id)
            let consoleLogger = new ConsoleLogger(showDate = true)
            
            { new ICloudLogger with
                  member x.Log(entry : string) : unit = 
                      consoleLogger.Log LogLevel.None entry
                      (cloudLogger :> ICloudLogger).Log(entry) }

    static member private GetDefaultResources(config : Configuration, customResources : ResourceRegistry, includeCache : bool) =
        let storeConn = config.StorageConnectionString
        let sbusConn = config.ServiceBusConnectionString
        let config = config.GetConfigurationId()
        let storeConfig = CloudFileStoreConfiguration.Create(BlobStore.Create(storeConn), config.UserDataContainer)
        let atomConfig = CloudAtomConfiguration.Create(AtomProvider.Create(storeConn), config.UserDataTable)
        let dictionaryConfig = CloudDictionaryProvider.Create(storeConn)
        let channelConfig = CloudChannelConfiguration.Create(ChannelProvider.Create(sbusConn))

        resource {
            yield storeConfig
            yield atomConfig
            yield dictionaryConfig
            yield channelConfig
            yield Config.Serializer
            if includeCache then 
                match customResources.TryResolve<Func<IObjectCache>>() with
                | None -> yield MBrace.Runtime.Store.InMemoryCache.Create()
                | Some factory -> yield factory.Invoke()
            yield! customResources
        }

    static member CreateForWorker(config : Configuration, workerId : IWorkerId, customLogger : ISystemLogger, customResources) =
        customLogger.LogInfof "Activating configuration with Id %A" config.Id
        Config.Activate(config, true)
        customLogger.LogInfof "Creating resources"
        let resources = RuntimeManager.GetDefaultResources(config, customResources, true)
        customLogger.LogInfof "Creating RuntimeManager for Worker %A" workerId
        let runtime = new RuntimeManager(config.GetConfigurationId(), workerId.Id, customLogger, resources)
        runtime.SetLocalWorkerId(workerId)
        runtime

    static member CreateForAppDomain(config : Configuration, workerId : IWorkerId, customLogger : ISystemLogger, customResources) =
        customLogger.LogInfof "Activating configuration with Id %A" config.Id
        Config.Activate(config, false)
        customLogger.LogInfof "Creating resources"
        let resources = RuntimeManager.GetDefaultResources(config, customResources, true)
        customLogger.LogInfof "Creating RuntimeManager for AppDomain %A" AppDomain.CurrentDomain.FriendlyName
        let runtime = new RuntimeManager(config.GetConfigurationId(), workerId.Id, customLogger, resources)
        runtime

    static member CreateForClient(config : Configuration, clientId : string, customLogger : ISystemLogger, customResources) =
        customLogger.LogInfof "Activating configuration with Id %A" config.Id
        Config.Activate(config, true)
        customLogger.LogInfof "Creating resources"        
        let resources = RuntimeManager.GetDefaultResources(config, customResources, false)
        customLogger.LogInfof "Creating RuntimeManager for Client %A" clientId
        let runtime = new RuntimeManager(config.GetConfigurationId(), clientId, customLogger, resources)
        runtime
