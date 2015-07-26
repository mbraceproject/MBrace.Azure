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
type RuntimeManager private (config : ConfigurationId, uuid : string, customLoggers : ISystemLogger seq, customResources : ResourceRegistry) = 
    let logger = new AttacheableLogger()
    do  
        let _ = logger.AttachLogger(StorageSystemLogger.Create(config, uuid))
        customLoggers |> Seq.map logger.AttachLogger |> ignore

    let runtimeId     = RuntimeId.FromConfiguration(config)
    let workerManager = WorkerManager.Create(config)
    let jobManager    = JobManager.Create(config)
    let taskManager   = TaskManager.Create(config)
    
    let assemblyManager = 
        let store = customResources.Resolve<CloudFileStoreConfiguration>()
        let serializer = customResources.Resolve<ISerializer>()
        StoreAssemblyManager.Create(store, serializer, "vagabond", logger)
    
    let cancellationEntryFactory = CancellationTokenFactory.Create(config)
    let int32CounterFactory = Int32CounterFactory.Create(config)
    let resultAggregatorFactory = ResultAggregatorFactory.Create(config)
    
    let resources = customResources

    member this.RuntimeManagerId = uuid

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

    static member CreateForWorker(config : ConfigurationId, workerId : IWorkerId, customLoggers) =
        let resources = ResourceRegistry.Empty
        let runtime = new RuntimeManager(config, workerId.Id, customLoggers, resources)
        runtime.SetJobQueueDefaultWorker(workerId)
        runtime

    static member CreateForClient(config : ConfigurationId, clientId : string, customLoggers) =
        new RuntimeManager(config, clientId, customLoggers, ResourceRegistry.Empty)