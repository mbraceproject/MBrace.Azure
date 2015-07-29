namespace MBrace.Azure.Runtime

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Azure
open System
open MBrace.Store.Internals
open MBrace.Azure.Store

type RuntimeId private (config : ConfigurationId) =

    member this.ConfigurationId = config

    interface IRuntimeId with
        member this.CompareTo(obj : obj) : int =
            match obj with
            | :? RuntimeId as rId -> compare rId.ConfigurationId this.ConfigurationId
            | _ -> 1

        member this.Id : string =
            Configuration.Pickler.Pickle(config)
            |> Convert.ToBase64String

    static member FromConfiguration(config) = new RuntimeId(config)

[<AutoSerializable(false)>]
type RuntimeManager private (config : ConfigurationId, uuid : string, customLoggers : ISystemLogger seq, resources : ResourceRegistry) =
    let logger = new AttacheableLogger()
    do customLoggers |> Seq.map logger.AttachLogger |> ignore

    let runtimeId     = RuntimeId.FromConfiguration(config)
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

    member private this.SetJobQueueDefaultWorker(workerId : IWorkerId) =
        jobManager.SetDefaultWorker(workerId)

    interface IRuntimeManager with
        member this.Id                       = runtimeId :> _
        member this.Serializer               = Configuration.Pickler :> _
        member this.WorkerManager            = workerManager :> _
        member this.TaskManager              = taskManager :> _
        member this.JobQueue                 = jobManager :> _
        member this.AssemblyManager          = assemblyManager :> _
        member this.SystemLogger             = logger :> _
        member this.CancellationEntryFactory = cancellationEntryFactory
        member this.CounterFactory           = int32CounterFactory
        member this.ResetClusterState()      = failwith "Not implemented yet"
        member this.ResourceRegistry         = resources
        member this.ResultAggregatorFactory  = resultAggregatorFactory
        member this.GetCloudLogger(worker : IWorkerId, job : CloudJob) =
            CloudStorageLogger(config, job.Id) :> _

    static member private GetDefaultResources(config : Configuration, customResources : ResourceRegistry, includeCache : bool) =
        let storeConfig = CloudFileStoreConfiguration.Create(BlobStore.Create(config.StorageConnectionString), config.UserDataContainer)
        let atomConfig = CloudAtomConfiguration.Create(AtomProvider.Create(config.StorageConnectionString), config.UserDataTable)
        let dictionaryConfig = CloudDictionaryProvider.Create(config.StorageConnectionString)
        let channelConfig = CloudChannelConfiguration.Create(ChannelProvider.Create(config.ServiceBusConnectionString))

        resource {
            yield storeConfig
            yield atomConfig
            yield dictionaryConfig
            yield channelConfig
            yield Configuration.Serializer
            if includeCache then 
                match customResources.TryResolve<Func<IObjectCache>>() with
                | None -> yield MBrace.Runtime.Store.InMemoryCache.Create()
                | Some factory -> yield factory.Invoke()
            yield! customResources
        }

    static member CreateForWorker(config : Configuration, workerId : IWorkerId, customLoggers, customResources) =
        let config = config.WithAppendedId
        Configuration.Activate(config)
        let resources = RuntimeManager.GetDefaultResources(config, customResources, true)
        let runtime = new RuntimeManager(config.ConfigurationId, workerId.Id, customLoggers, resources)
        runtime.SetJobQueueDefaultWorker(workerId)
        runtime

    static member CreateForClient(config : Configuration, clientId : string, customLoggers, customResources) =
        let config = config.WithAppendedId
        Configuration.Activate(config)
        let resources = RuntimeManager.GetDefaultResources(config, customResources, false)
        let runtime = new RuntimeManager(config.ConfigurationId, clientId, customLoggers, resources)
        runtime
