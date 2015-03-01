namespace MBrace.Azure.Runtime
open MBrace
open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common
open MBrace.Azure.Store
open MBrace.Continuation
open MBrace.Runtime
open MBrace.Runtime.Store
open MBrace.Store
open System
open System.Diagnostics
open System.Threading

module private ReleaseInfo =
    open System.Reflection

    let prettyPrint () =
        let asm = Assembly.GetExecutingAssembly()
        let attributes = 
            asm.GetCustomAttributes<AssemblyMetadataAttribute>()
            |> Seq.map (fun ma -> ma.Key, ma.Value)
            |> Map.ofSeq
        attributes.["Release Signature"]

/// MBrace Runtime Service.
type Service (config : Configuration, serviceId : string) =
    // TODO : Add locks
    let mutable storeProvider   = None
    let mutable channelProvider = None
    let mutable atomProvider    = None
    let mutable cache           = None
    let mutable resources       = ResourceRegistry.Empty
    let mutable configuration   = config
    let mutable maxJobs         = Environment.ProcessorCount
    let logger                  = new LoggerCombiner() 
    let worker                  = new Worker()
    
    let logf fmt = logger.Logf fmt
    let check () = if worker.IsActive then invalidOp "Cannot change Service while it is active."
    
    /// MBrace Runtime Service.
    new(config : Configuration) = new Service (config, guid())

    member __.Id = serviceId
    member __.AttachLogger(l) = check(); logger.Attach(l)
    
    member __.Configuration  
        with get () = configuration
        and set c = check (); configuration <- c

    member __.MaxConcurrentJobs 
        with get () = maxJobs
        and set c = check (); maxJobs <- c
    
    member __.RegisterStoreProvider(store : ICloudFileStore) =
        check () ; storeProvider <- Some store
    
    member __.RegisterAtomProvider(atom : ICloudAtomProvider) = 
        check () ; atomProvider <- Some atom
    
    member __.RegisterChannelProvider(channel : ICloudChannelProvider) = 
        check () ; channelProvider <- Some channel
    
    member __.RegisterCache(cacheStore : ICloudFileStore) =
        check () ; cache <- Some <| cacheStore

    member __.RegisterResource(resource : 'TResource) = check () ; resources <- resources.Register(resource)
    
    member __.StartAsTask() : Tasks.Task = Async.StartAsTask(__.StartAsync()) :> _
    member __.StartAsTask(ct : CancellationToken) : Tasks.Task = Async.StartAsTask(__.StartAsync(), cancellationToken = ct) :> _     
    member __.StartAsync() : Async<unit> =
        async {
            try
                logf "Starting Service %s" serviceId
                let sw = new Stopwatch() in sw.Start()

                let cfg = __.Configuration.WithAppendedId

                logf "Activating Configuration %010d, Hash %d" cfg.Id (hash cfg.ConfigurationId)
                Configuration.AddIgnoredAssembly(typeof<Service>.Assembly)
                do! Configuration.ActivateAsync(cfg)

                logf "Creating storage logger"
                let storageLogger = new StorageLogger(cfg.ConfigurationId, Worker(id = __.Id))
                logger.Attach(storageLogger)

                logf "%s" <| ReleaseInfo.prettyPrint()

                let serializer = Configuration.Serializer
                logf "Serializer : %s" serializer.Id

                logf "Initializing RuntimeState"
                let! state = RuntimeState.FromConfiguration(cfg)

                storeProvider <- Some(defaultArg storeProvider (BlobStore.Create(cfg.StorageConnectionString) :> _))
                logf "CloudFileStore : %s" storeProvider.Value.Id

                logf "Creating InMemoryCache"
                let inMemoryCache = InMemoryCache.Create()
                
                cache <- Some(defaultArg cache (FileSystemStore.CreateSharedLocal() :> ICloudFileStore))

                logf "Local Cache Store %s" cache.Value.Id
                let store = FileStoreCache.Create(storeProvider.Value, cache.Value) :> ICloudFileStore
                logf "CachedStore %s created" store.Id

                atomProvider <- Some(defaultArg atomProvider ({ new AtomProvider(cfg.StorageConnectionString, Configuration.Serializer) with
                                                                    override __.ComputeSize(value : 'T) = Configuration.Pickler.ComputeSize(value) } :> ICloudAtomProvider))
                logf "AtomProvider : %s" atomProvider.Value.Id

                channelProvider <- Some( defaultArg channelProvider (ChannelProvider.Create(cfg.ServiceBusConnectionString, Configuration.Serializer)))
                logf "ChannelProvider : %s" channelProvider.Value.Id

                let wmon = WorkerManager.Create(cfg.ConfigurationId, MaxJobs = __.MaxConcurrentJobs)
                let! e = wmon.RegisterCurrent(serviceId)
                logf "Declared node : %s \nPID : %d \nServiceId : %s" e.Hostname e.ProcessId (e :> IWorkerRef).Id
                
                Async.Start(wmon.HeartbeatLoop())
                logf "Started heartbeat loop" 

                let resources = resource { 
                    yield! resources
                    yield serializer
                    yield logger
                    yield wmon
                    yield state.ProcessMonitor 
                }
                state.JobQueue.Affinity <- serviceId
                logf "Subscription for %s created" serviceId

                logf "MaxConcurrentJobs : %d" __.MaxConcurrentJobs
                logf "Starting worker loop"
                let config = { 
                    State              = state
                    MaxConcurrentJobs  = __.MaxConcurrentJobs
                    Resources          = resources
                    Store              = store
                    Channel            = channelProvider.Value
                    Atom               = atomProvider.Value
                    Cache              = inMemoryCache
                    Logger             = logger
                    WorkerMonitor      = wmon
                    ProcessMonitor     = state.ProcessMonitor 
                }
                let! handle = Async.StartChild <| async { do worker.Start(config) }
                logf "Worker loop started"
                
                sw.Stop()
                logf "Service %s started in %.3f seconds" serviceId sw.Elapsed.TotalSeconds
                return! handle
            with ex ->
                logf "Service Start for %s failed with %A" __.Id  ex
                return! Async.Raise ex
        }

    member __.Start() = Async.RunSync(__.StartAsync())

    member __.StopAsync () =
        async {
            try
                logf "Stopping Service %s." serviceId
                //TODO : Add other finalizations.
                worker.Stop()
                logf "Service %s stopped." serviceId
            with ex ->
                logf "Service Stop for %s failed with %A" __.Id  ex
                return! Async.Raise ex
        }

    member __.Stop () = Async.RunSync(__.StopAsync())