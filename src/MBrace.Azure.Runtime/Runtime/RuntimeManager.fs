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
type RuntimeManager (config : ConfigurationId, logger : ISystemLogger, resources : ResourceRegistry) =
    
    let runtimeId = RuntimeId.FromConfiguration(config)
    let workerManager = WorkerManager.Create(config)
    let jobManager = JobManager.Create(config)
    let taskManager = TaskManager.Create(config)
    let assemblyManager = 
        let store = resources.Resolve<CloudFileStoreConfiguration>()
        let serializer = resources.Resolve<ISerializer>()
        StoreAssemblyManager.Create(store, serializer, "vagabond", logger)

    interface IRuntimeManager with
        member this.Id = runtimeId :> _
        member this.Serializer = Configuration.Pickler :> _
        
        member this.WorkerManager: IWorkerManager = workerManager :> _
        member this.TaskManager: ICloudTaskManager = taskManager :> _
        member this.JobQueue: IJobQueue = jobManager :> _
        member this.AssemblyManager: IAssemblyManager = assemblyManager :> _
        
        member this.SystemLogger: ISystemLogger = logger

        member this.GetCloudLogger(worker: IWorkerId, job: CloudJob): ICloudLogger = 
            let pl = new MBrace.Azure.Runtime.Info.ProcessLogger(config, job.TaskEntry.Id) 
            let lc = new MBrace.Azure.Runtime.Info.RuntimeLogger()
            lc.Attach(new ConsoleLogger())
            lc.Attach(pl)
            lc :> _


        
        member this.CancellationEntryFactory: ICancellationEntryFactory = 
            failwith "Not implemented yet"
        
        member this.CounterFactory: ICloudCounterFactory = 
            failwith "Not implemented yet"
        
        member this.ResetClusterState(): Async<unit> = 
            failwith "Not implemented yet"
        
        member this.ResourceRegistry: ResourceRegistry = 
            failwith "Not implemented yet"
        
        member this.ResultAggregatorFactory: ICloudResultAggregatorFactory = 
            failwith "Not implemented yet"
        
        
        