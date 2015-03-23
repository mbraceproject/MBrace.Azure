namespace MBrace.Azure.Runtime
open MBrace
open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Primitives
open MBrace.Azure.Store
open MBrace.Continuation
open MBrace.Runtime
open MBrace.Runtime.Store
open MBrace.Store
open System
open System.Diagnostics
open System.Threading
open MBrace.Azure.Runtime.Info
open MBrace.Azure.Runtime.Utilities


/// MBrace Runtime Service.
type Service (config : Configuration, serviceId : string) =
    // TODO : Add locks
    let mutable storeProvider   = None
    let mutable channelProvider = None
    let mutable atomProvider    = None
    let mutable cache           = None
    let mutable useCache        = false
    let mutable customResources = ResourceRegistry.Empty
    let mutable configuration   = config
    let mutable maxJobs         = Environment.ProcessorCount
    let customLogger            = new LoggerCombiner() 
    let worker                  = new Worker()
    
    let logf fmt = customLogger.Logf fmt
    let check () = if worker.IsActive then invalidOp "Cannot change Service while it is active."
    
    /// MBrace Runtime Service.
    new(config : Configuration) = new Service (config, guid())

    /// Get service's unique identifier.
    member this.Id = serviceId
    
    /// Attach logger to worker.
    member this.AttachLogger(l) = check(); customLogger.Attach(l)
    
    /// Get or set service configuration.
    member this.Configuration  
        with get () = configuration
        and set c = check (); configuration <- c
    
    /// Determines if the registered local cache will be used.
    member this.UseLocalCache
        with get () = useCache
        and set c = check (); useCache <- c

    /// Get or set the maximum number of jobs that this worker may execute concurrently.
    member this.MaxConcurrentJobs 
        with get () = maxJobs
        and set c = check (); maxJobs <- c
    
    /// Register an ICloudFileStore instance. Defaults to BlobStore with configuration's storage connection string.
    member this.RegisterStoreProvider(store : ICloudFileStore) =
        check () ; storeProvider <- Some store
    
    /// Register an ICloudAtomProvider instance. Defaults to table store implementation with configuration's storage connection string.
    member this.RegisterAtomProvider(atom : ICloudAtomProvider) = 
        check () ; atomProvider <- Some atom
    
    /// Register an ICloudChannelProvider instance. Defaults to Service Bus queue implementation with configuration's Service Bus connection string.
    member this.RegisterChannelProvider(channel : ICloudChannelProvider) = 
        check () ; channelProvider <- Some channel
    
    /// Register a local filesystem cache implementation. Defaults to FileSystemStore in local TEMP folder.
    member this.RegisterCache(cacheStore : ICloudFileStore) =
        check () ; cache <- Some <| cacheStore

    /// Add a custom resource in workers ResourceRegistry.
    member this.RegisterResource(resource : 'TResource) = check () ; customResources <- customResources.Register(resource)
    
    /// Start Service and worker loop as a Task.
    member this.StartAsTask() : Tasks.Task = Async.StartAsTask(this.StartAsync()) :> _
    
    /// Start Service and worker loop as a Task.
    member this.StartAsTask(ct : CancellationToken) : Tasks.Task = Async.StartAsTask(this.StartAsync(), cancellationToken = ct) :> _     
    
    /// Asynchronously start Service and worker loop.
    member this.StartAsync() : Async<unit> =
        async {
            try
                let sw = new Stopwatch() in sw.Start()

                let! state, resources = Init.Initializer(config, serviceId, true, customLogger, maxJobs = this.MaxConcurrentJobs)

                storeProvider <- Some(defaultArg storeProvider (BlobStore.Create(config.StorageConnectionString) :> _))
                logf "CloudFileStore : %s" storeProvider.Value.Id

                let store = 
                    if this.UseLocalCache then
                        cache <- 
                            Some <|
                                match cache with
                                | Some cs -> cs
                                | None -> FileSystemStore.CreateSharedLocal() :> ICloudFileStore

                        logf "Local Cache Store %s" cache.Value.Id
                        let store = FileStoreCache.Create(storeProvider.Value, cache.Value) :> ICloudFileStore
                        logf "CachedStore %s created" store.Id
                        store
                    else
                        storeProvider.Value

                atomProvider <- 
                    Some <|
                        match atomProvider with
                        | Some ap -> ap
                        | None -> AtomProvider.Create(config.StorageConnectionString) :> _

                logf "AtomProvider : %s" atomProvider.Value.Id

                channelProvider <- 
                    Some <| 
                        match channelProvider with
                        | Some cp -> cp
                        | None -> ChannelProvider.Create(config.ServiceBusConnectionString) :> _

                logf "ChannelProvider : %s" channelProvider.Value.Id

                logf "MaxConcurrentJobs : %d" this.MaxConcurrentJobs

                logf "Initializing JobEvaluator"
                let jobEvaluator = new JobEvaluator(config, serviceId, customLogger)
                
                logf "Starting worker loop"
                let config = { 
                    State                     = state
                    JobEvaluator              = jobEvaluator
                    MaxConcurrentJobs         = this.MaxConcurrentJobs
                    Resources                 = resources
                    JobEvaluatorConfiguration =
                        { Store               = store
                          Channel             = channelProvider.Value
                          Atom                = atomProvider.Value
                          CustomResources     = customResources }
                    Logger                    = customLogger
                }
                let! handle = Async.StartChild <| async { do worker.Start(config) }
                logf "Worker loop started"
                
                sw.Stop()
                logf "Service %s started in %.3f seconds" serviceId sw.Elapsed.TotalSeconds
                return! handle
            with ex ->
                logf "Service Start for %s failed with %A" this.Id  ex
                return! Async.Raise ex
        }

    /// Start Service. This method is blocking until worker loop completes.
    member this.Start() = Async.RunSync(this.StartAsync())

    /// Stop Service and worker loop. Wait for any pending jobs.
    member this.StopAsync () =
        async {
            try
                logf "Stopping Service %s." serviceId
                //TODO : Add other finalizations.
                worker.Stop()
                logf "Service %s stopped." serviceId
            with ex ->
                logf "Service Stop for %s failed with %A" this.Id  ex
                return! Async.Raise ex
        }

    /// Stop Service and worker loop. Wait for any pending jobs.
    member this.Stop () = Async.RunSync(this.StopAsync())