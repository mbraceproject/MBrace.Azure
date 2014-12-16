namespace Nessos.MBrace.Azure.Runtime

open System
open System.Threading
open System.Threading.Tasks
open Nessos.MBrace.Azure.Runtime
open System.Runtime.InteropServices
open Nessos.MBrace.Azure.Runtime.Common
open Nessos.MBrace
open Nessos.MBrace.Runtime
open Nessos.MBrace.Continuation
open Nessos.MBrace.Store
open Nessos.MBrace.Azure.Runtime.Resources
open System.Diagnostics
open Nessos.MBrace.Azure.Store

/// MBrace Runtime Service.
type Service (config : Configuration, serviceId : string) =
    // TODO : Add locks
    let mutable running = false
    let mutable storeProvider   = None
    let mutable channelProvider = None
    let mutable atomProvider    = None
    let mutable resources = ResourceRegistry.Empty
    let logger = new NullLogger() :> ILogger
    let logf fmt = Printf.ksprintf logger.Log fmt
    let check () = if running then failwith "Service is active"
    
    /// MBrace Runtime Service.
    new(config : Configuration) = new Service (config, guid())

    member __.Configuration = config
    member __.Id = serviceId
    member __.AttachLogger(l) = check(); logger.Attach(l)
    
    member val MaxConcurrentTasks = Environment.ProcessorCount with get, set
    
    member __.RegisterStoreProvider(store : ICloudFileStore) =  check () ; storeProvider <- Some store
    member __.RegisterAtomProvider(atom : ICloudAtomProvider) = check () ; atomProvider <- Some atom
    member __.RegisterChannelProvider(channel : ICloudChannelProvider) = check () ; channelProvider <- Some channel
    member __.RegisterResource(resource : 'TResource) = check () ; resources <- resources.Register(resource)
    
    member __.StartAsTask() : Tasks.Task = Async.StartAsTask(__.StartAsync()) :> _
    member __.StartAsTask(ct : CancellationToken) : Tasks.Task = Async.StartAsTask(__.StartAsync(), cancellationToken = ct) :> _     
    member __.StartAsync() : Async<unit> =
        async {
            try
                logf "Starting Service %s" serviceId
                let sw = new Stopwatch() in sw.Start()

                logf "Activating Configuration"
                do! Configuration.Activate(config)

                logf "Creating storage logger"
                let storageLogger = new StorageLogger(config.ConfigurationId, config.DefaultLogTable, Worker(id = __.Id))
                logger.Attach(storageLogger)
                
                let serializer = Configuration.Serializer
                logf "Serializer : %s" serializer.Id

                // Fork RuntimeState initialization for speedup
                logf "Initializing RuntimeState"
                let! stateHandle = Async.StartChild(RuntimeState.FromConfiguration(config))

                storeProvider <- Some(defaultArg storeProvider (BlobStore(config.StorageConnectionString) :> _))
                logf "CloudFileStore : %s" storeProvider.Value.Id

                atomProvider <- Some(defaultArg atomProvider (AtomProvider.Create(config.ConfigurationId)))
                logf "AtomProvider : %s" atomProvider.Value.Id

                channelProvider <- Some( defaultArg channelProvider (ChannelProvider.Create(config.ConfigurationId)))
                logf "ChannelProvider : %s" channelProvider.Value.Id

                let wmon = WorkerMonitor.Create(config)
                let! e = wmon.DeclareCurrent(serviceId)
                logf "Declared node %s : %d : %s" e.Hostname e.ProcessId (e :> IWorkerRef).Id
                
                Async.Start(wmon.HeartbeatLoop())
                logf "Started heartbeat loop" 

                let pmon = ProcessMonitor.Create(config)
                logf "ProcessMonitor created"


                let! state = stateHandle
                let resources = resource { yield! resources; yield serializer; yield logger; yield wmon; yield state.ProcessMonitor }
                state.TaskQueue.Affinity <- serviceId
                logf "Subscription for %s created" serviceId

                logf "Starting worker loop"
                logf "MaxConcurrentTasks : %d" __.MaxConcurrentTasks
                let! handle = 
                    Worker.initWorker state __.MaxConcurrentTasks resources storeProvider.Value channelProvider.Value atomProvider.Value 
                    |> Async.StartChild
                logf "Worker loop started"
                
                sw.Stop()
                logf "Service %s started in %.3f seconds" serviceId sw.Elapsed.TotalSeconds
                return! handle
            with ex ->
                logf "Service %s failed with %A" __.Id  ex
                return! Async.Raise ex
        }

    member __.Start() = Async.RunSync(__.StartAsync())
