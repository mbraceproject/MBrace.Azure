namespace MBrace.Azure.Runtime

open System
open System.Threading
open System.Threading.Tasks
open MBrace.Azure.Runtime
open System.Runtime.InteropServices
open MBrace.Azure.Runtime.Common
open MBrace
open MBrace.Runtime
open MBrace.Continuation
open MBrace.Store
open MBrace.Azure.Runtime.Resources
open System.Diagnostics
open MBrace.Azure.Store
open MBrace.Runtime.Store

/// MBrace Runtime Service.
type Service (config : Configuration, serviceId : string) =
    // TODO : Add locks
    let mutable running = false
    let mutable storeProvider   = None
    let mutable channelProvider = None
    let mutable atomProvider    = None
    let mutable cache           = None
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
    
    member __.RegisterStoreProvider(store : ICloudFileStore) =
        check () ; storeProvider <- Some store
    
    member __.RegisterAtomProvider(atom : ICloudAtomProvider) = 
        check () ; atomProvider <- Some atom
    
    member __.RegisterChannelProvider(channel : ICloudChannelProvider) = 
        check () ; channelProvider <- Some channel
    
    member __.RegisterCache(cacheStore : ICloudFileStore) =
        check () ; cache <- Some <| FileStoreCache.CreateCachedStore(cacheStore)

    member __.RegisterResource(resource : 'TResource) = check () ; resources <- resources.Register(resource)
    
    member __.StartAsTask() : Tasks.Task = Async.StartAsTask(__.StartAsync()) :> _
    member __.StartAsTask(ct : CancellationToken) : Tasks.Task = Async.StartAsTask(__.StartAsync(), cancellationToken = ct) :> _     
    member __.StartAsync() : Async<unit> =
        async {
            try
                logf "Starting Service %s" serviceId
                let sw = new Stopwatch() in sw.Start()

                logf "Activating Configuration"
                do! Configuration.ActivateAsync(config)

                logf "Creating storage logger"
                let storageLogger = new StorageLogger(config.ConfigurationId, config.DefaultLogTable, Worker(id = __.Id))
                logger.Attach(storageLogger)
                
                let serializer = Configuration.Serializer
                logf "Serializer : %s" serializer.Id

                // Fork RuntimeState initialization for speedup
                logf "Initializing RuntimeState"
                let! stateHandle = Async.StartChild(RuntimeState.FromConfiguration(config))

                storeProvider <- Some(defaultArg storeProvider (BlobStore.Create(config.StorageConnectionString) :> _))
                logf "CloudFileStore : %s" storeProvider.Value.Id

                logf "Creating InMemoryCache"
                InMemoryCacheRegistry.SetCache (InMemoryCache.Create())
                
                match cache with 
                | None -> 
                    logf "Registering local filesystem cache"
                    FileStoreCache.RegisterLocalFileSystemCache()
                | Some c -> 
                    logf "Registering cache store : %s" c.SourceStore.Id
                    FileStoreCache.RegisterLocalCacheStore(c)

                let store = FileStoreCache.CreateCachedStore(storeProvider.Value) :> ICloudFileStore
                logf "CachedStore created"

                atomProvider <- Some(defaultArg atomProvider ({ new AtomProvider(config.StorageConnectionString, Configuration.Serializer) with
                                                                    override __.ComputeSize(value : 'T) = Configuration.Pickler.ComputeSize(value) } :> ICloudAtomProvider))
                logf "AtomProvider : %s" atomProvider.Value.Id

                channelProvider <- Some( defaultArg channelProvider (ChannelProvider.Create(config.ServiceBusConnectionString, Configuration.Serializer)))
                logf "ChannelProvider : %s" channelProvider.Value.Id

                let wmon = WorkerMonitor.Create(config, MaxTasks = __.MaxConcurrentTasks)
                let! e = wmon.DeclareCurrent(serviceId)
                logf "Declared node\n\tHostname \"%s\"\n\tPID:\"%d\"\n\tServiceId:\"%s\"" e.Hostname e.ProcessId (e :> IWorkerRef).Id
                
                Async.Start(wmon.HeartbeatLoop())
                logf "Started heartbeat loop" 

                let! state = stateHandle
                let resources = resource { yield! resources; yield serializer; yield logger; yield wmon; yield state.ProcessMonitor }
                state.TaskQueue.Affinity <- serviceId
                logf "Subscription for %s created" serviceId

                logf "Starting worker loop"
                logf "MaxConcurrentTasks : %d" __.MaxConcurrentTasks
                let! handle = 
                    Worker.initWorker state __.MaxConcurrentTasks resources store channelProvider.Value atomProvider.Value 
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
