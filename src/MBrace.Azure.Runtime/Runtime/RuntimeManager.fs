namespace MBrace.Azure.Runtime

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Azure
open System

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
type RuntimeManager () =
    interface IRuntimeManager with
        member this.AssemblyManager: IAssemblyManager = 
            failwith "Not implemented yet"
        
        member this.CancellationEntryFactory: ICancellationEntryFactory = 
            failwith "Not implemented yet"
        
        member this.CounterFactory: ICloudCounterFactory = 
            failwith "Not implemented yet"
        
        member this.GetCloudLogger(worker: IWorkerId, job: CloudJob): ICloudLogger = 
            failwith "Not implemented yet"
        
        member this.Id: IRuntimeId = 
            failwith "Not implemented yet"
        
        member this.JobQueue: IJobQueue = 
            failwith "Not implemented yet"
        
        member this.ResetClusterState(): Async<unit> = 
            failwith "Not implemented yet"
        
        member this.ResourceRegistry: ResourceRegistry = 
            failwith "Not implemented yet"
        
        member this.ResultAggregatorFactory: ICloudResultAggregatorFactory = 
            failwith "Not implemented yet"
        
        member this.Serializer: Nessos.FsPickler.FsPicklerSerializer = 
            failwith "Not implemented yet"
        
        member this.SystemLogger: ISystemLogger = 
            failwith "Not implemented yet"
        
        member this.TaskManager: ICloudTaskManager = 
            failwith "Not implemented yet"
        
        member this.WorkerManager: IWorkerManager = 
            failwith "Not implemented yet"
        