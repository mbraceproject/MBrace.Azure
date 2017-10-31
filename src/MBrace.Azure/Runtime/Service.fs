namespace MBrace.Azure.Service

open System
open System.IO
open System.Diagnostics
open System.Threading

open MBrace.FsPickler

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities

/// MBrace worker service execution status
type ServiceStatus =
    | Stopped = 0
    | Starting = 1
    | Running = 2
    | Stopping = 3

/// MBrace.Azure worker service manager
[<Sealed; AutoSerializable(false)>]
type WorkerService (config : Configuration, workerId : string) =
    do Validate.subscriptionName workerId
    let mutable useAppDomainIsolation   = true
    let mutable customResources         = ResourceRegistry.Empty
    let mutable configuration           = FsPickler.Clone config
    let mutable maxWorkItems            = Environment.ProcessorCount
    let mutable heartbeatThreshold      = TimeSpan.FromMinutes 5.
    let mutable heartbeatInterval       = TimeSpan.FromSeconds 2.
    let mutable workingDirectory        = WorkingDirectory.GetDefaultWorkingDirectoryForProcess(prefix = "mbrace.azure")
    let mutable logFile                 = None
    let mutable status                  = 0 // 0: stopped, 1: starting, 2: running, 3: stopping
    let mutable subscription            = None : WorkerSubscription.Subscription option
    let mutable cancellationTokenSource = None : CancellationTokenSource option
    let attacheableLogger               = AttacheableLogger.Create(logLevel = LogLevel.Info, makeAsynchronous = true)

    let trySetStarting  () = Interlocked.CompareExchange(&status, 1, 0) = 0
    let trySetStopping  () = Interlocked.CompareExchange(&status, 3, 2) = 2
    let checkStopped    () = if status <> 0 then invalidOp "Cannot change Service while it is active."
    let validateInterval (i : TimeSpan) = if i < TimeSpan.FromSeconds 1. then invalidArg "interval" "Must be at least 1 second." else i

    /// MBrace Runtime Service.
    new(config : Configuration) = new WorkerService (config, guid())

    /// Get service unique identifier.
    member this.Id = workerId
    
    /// Attach logger to worker. Return an unsubscribe token.
    member this.AttachLogger(logger : ISystemLogger) = checkStopped(); attacheableLogger.AttachLogger(logger)

    /// Gets or sets the logfile used by the service.
    member this.LogFile
        with get () = defaultArg logFile null
        and set l = checkStopped (); ignore <| Path.GetFullPath l ; logFile <- Some l

    /// Get or set the logger verbosity.
    member this.LogLevel
        with get () = attacheableLogger.LogLevel
        and  set l = checkStopped (); attacheableLogger.LogLevel <- l

    /// Get or sets the worker heartbeat update interval
    member this.HeartbeatInterval
        with get () = heartbeatInterval
        and set i = checkStopped (); heartbeatInterval <- validateInterval i

    /// Get or sets the worker heartbeat update interval
    member this.HeartbeatThreshold
        with get () = heartbeatThreshold
        and set i = checkStopped (); heartbeatThreshold <- validateInterval i

    /// Gets or sets the working directory for the worker process
    member this.WorkingDirectory
        with get () = workingDirectory
        and set wd = checkStopped (); workingDirectory <- Path.GetFullPath wd

    /// Get or set service configuration.
    member this.Configuration  
        with get () = configuration
        and set c = checkStopped (); configuration <- FsPickler.Clone c

    /// Get or set off AppDomain isolation will be used.
    member this.UseAppDomainIsolation 
        with get () = useAppDomainIsolation
        and set c = checkStopped (); useAppDomainIsolation <- c

    /// Get or set the maximum number of jobs that this worker may execute concurrently.
    member this.MaxConcurrentWorkItems 
        with get () = maxWorkItems
        and set c = checkStopped (); maxWorkItems <- c
    
    /// Gets the current service status
    member this.Status : ServiceStatus = enum status

    /// Register a CloudFileStoreConfiguration instance. Defaults to BlobStore with configuration's storage connection string.
    member this.RegisterStoreConfiguration(store : ICloudFileStore) =
        checkStopped () ; customResources <- customResources.Register(store)

    /// Register a CloudAtomConfiguration instance. Defaults to table store implementation with configuration's storage connection string.
    member this.RegisterAtomProvider(atom : ICloudAtomProvider) = 
        checkStopped () ; customResources <- customResources.Register(atom)
    
    /// Register a CloudQueueConfiguration instance. Defaults to Service Bus queue implementation with configuration's Service Bus connection string.
    member this.RegisterQueueProvider(channel : ICloudQueueProvider) = 
        checkStopped () ; customResources <- customResources.Register(channel)

    /// Register an ICloudChannelProvider instance. Defaults to Service Bus queue implementation with configuration's Service Bus connection string.
    member this.RegisterDictionaryProvider(dictionary : ICloudDictionaryProvider) = 
        checkStopped () ; customResources <- customResources.Register(dictionary)

    /// Add a custom resource in workers ResourceRegistry.
    member this.RegisterResource(resource : 'TResource) = 
        checkStopped () ; customResources <- customResources.Register(resource)
    
    /// Asynchronously starts the service with specified configuration
    member this.StartAsync() : Async<unit> = async {
        if not <| trySetStarting() then invalidOp "MBrace.Azure.Service is already running."
        try
            let sw = Stopwatch.StartNew()
            attacheableLogger.LogInfof "Starting MBrace.Azure.Service %A" workerId
            if not ProcessConfiguration.IsInitialized then
                ProcessConfiguration.InitAsWorker(this.WorkingDirectory)

            match logFile with
            | Some lf ->
                let fullPath = Path.Combine(workingDirectory, lf) |> Path.GetFullPath
                let logger = FileSystemLogger.Create(fullPath, showDate = true, append = true)
                let _ = attacheableLogger.AttachLogger logger in ()
            | None -> ()

            let! sub = WorkerSubscription.initialize config this.Id attacheableLogger heartbeatInterval heartbeatThreshold this.UseAppDomainIsolation this.MaxConcurrentWorkItems customResources
            sw.Stop()
            attacheableLogger.LogInfof "Service %A started in %.3f seconds" workerId sw.Elapsed.TotalSeconds
            subscription <- Some sub
            status <- int ServiceStatus.Running 
            return ()
        with e ->
            subscription |> Option.iter (fun sub -> try sub.Dispose() with _ -> ())
            subscription <- None
            status <- int ServiceStatus.Stopped
            attacheableLogger.LogErrorf "Service Start for %A failed with %A" this.Id e
            return! Async.Raise e
    }

    /// Asynchronously starts the service with specified configuration
    member this.StartAsTask() = this.StartAsync() |> Async.StartAsTask

    /// Synchronously starts the worker service with specified configuration
    member this.Start() = Async.RunSync(this.StartAsync())

    /// Synchronously starts the worker service with specified configuration and blocks the calling thread until stopped.
    member this.Run() : unit =
        this.Start()
        let cts = new CancellationTokenSource()
        cancellationTokenSource <- Some cts
        Thread.Diverge(cts.Token)

    /// Asynchronously stops the worker service
    member this.StopAsync ()  : Async<unit> = async {
        if not <| trySetStopping() then invalidOp "MBrace.Azure.Service is not running."
        try
            attacheableLogger.LogInfof "Stopping Service %A." workerId
            subscription |> Option.iter (fun s -> s.Dispose())
            cancellationTokenSource |> Option.iter (fun c -> c.Cancel())
            subscription <- None
            cancellationTokenSource <- None
            status <- int ServiceStatus.Stopped
            attacheableLogger.LogInfof "Service %A stopped." workerId
        with e ->
            status <- int ServiceStatus.Running
            attacheableLogger.LogErrorf "Service Stop for %A failed with %A" this.Id e
            return! Async.Raise e
    }

    /// Asynchronously stops the worker service
    member this.StopAsTask() = this.StopAsync() |> Async.StartAsTask

    /// Synchronously stops the worker service
    member this.Stop () = Async.RunSync(this.StopAsync())