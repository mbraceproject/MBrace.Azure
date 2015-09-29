namespace MBrace.Azure.Runtime

open System
open System.Diagnostics
open System.Threading

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities

// TODO : make thread safe, overall clean up

/// MBrace Runtime Service.
type Service (config : Configuration, serviceId : string) =
    let mutable useAppDomainIsolation   = true
    let mutable ignoreVersion           = true
    let mutable customResources         = ResourceRegistry.Empty
    let mutable configuration           = config
    let mutable maxWorkItems            = Environment.ProcessorCount
    let mutable subscription            = None : WorkerSubscription.Subscription option
    let attachableLogger                = AttacheableLogger.Create(makeAsynchronous = true)
    
    let check () = 
        match subscription with
        | Some wa when wa.Agent.IsRunning -> invalidOp "Cannot change Service while it is active."
        | _ -> ()

    /// MBrace Runtime Service.
    new(config : Configuration) = new Service (config, guid())

    /// Get service's unique identifier.
    member this.Id = serviceId
    
    /// Attach logger to worker. Return an unsubscribe token.
    member this.AttachLogger(logger : ISystemLogger) = check(); attachableLogger.AttachLogger(logger)
    
    /// Get or set the logger verbosity.
    member this.LogLevel
        with get () = attachableLogger.LogLevel
        and  set l = check (); attachableLogger.LogLevel <- l

    /// Get or set service configuration.
    member this.Configuration  
        with get () = configuration
        and set c = check (); configuration <- c

    /// Get or set off AppDomain isolation will be used.
    member this.UseAppDomainIsolation 
        with get () = useAppDomainIsolation
        and set c = check (); useAppDomainIsolation <- c

    /// Get or set iff version compatibility will be ignored.
    member this.IgnoreVersionCompatibility
        with get () = ignoreVersion
        and set c = check (); ignoreVersion <- c

    /// Get or set the maximum number of jobs that this worker may execute concurrently.
    member this.MaxConcurrentWorkItems 
        with get () = maxWorkItems
        and set c = check (); maxWorkItems <- c
    
    member this.IsRunning 
        with get () =
            match subscription with
            | Some wa -> wa.Agent.IsRunning
            | None -> false

    /// Register a CloudFileStoreConfiguration instance. Defaults to BlobStore with configuration's storage connection string.
    member this.RegisterStoreConfiguration(store : ICloudFileStore) =
        check () ; customResources <- customResources.Register(store)

    /// Register a CloudAtomConfiguration instance. Defaults to table store implementation with configuration's storage connection string.
    member this.RegisterAtomProvider(atom : ICloudAtomProvider) = 
        check () ; customResources <- customResources.Register(atom)
    
    /// Register a CloudQueueConfiguration instance. Defaults to Service Bus queue implementation with configuration's Service Bus connection string.
    member this.RegisterQueueProvider(channel : ICloudQueueProvider) = 
        check () ; customResources <- customResources.Register(channel)

    /// Register an ICloudChannelProvider instance. Defaults to Service Bus queue implementation with configuration's Service Bus connection string.
    member this.RegisterDictionaryProvider(dictionary : ICloudDictionaryProvider) = 
        check () ; customResources <- customResources.Register(dictionary)

    /// Add a custom resource in workers ResourceRegistry.
    member this.RegisterResource(resource : 'TResource) = 
        check () ; customResources <- customResources.Register(resource)
    
    /// Start Service and worker loop as a Task.
    member this.StartAsTask() : Tasks.Task = Async.StartAsTask(this.StartAsync()) :> _
    
    /// Start Service and worker loop as a Task.
    member this.StartAsTask(ct : CancellationToken) : Tasks.Task = Async.StartAsTask(this.StartAsync(), cancellationToken = ct) :> _     
    
    /// Asynchronously start Service and worker loop.
    member this.StartAsync() : Async<unit> = async {
        try
            let sw = Stopwatch.StartNew()
            let config2 = ClusterId.Activate config
            let tableLogManager = new TableSystemLogManager(config2)
            let! tableLogger = tableLogManager.CreateLogWriter(serviceId)
            let _ = attachableLogger.AttachLogger(tableLogger)

            attachableLogger.LogInfof "Starting MBrace.Azure.Runtime.Service %A" serviceId

            let! sub = WorkerSubscription.initialize config this.Id attachableLogger this.UseAppDomainIsolation this.MaxConcurrentWorkItems customResources
            subscription <- Some sub
            sw.Stop()
            attachableLogger.LogInfof "Service %A started in %.3f seconds" serviceId sw.Elapsed.TotalSeconds
            return ()
        with ex ->

            attachableLogger.LogErrorf "Service Start for %A failed with %A" this.Id ex
            // TODO : finalize
            return! Async.Raise ex
    }

    /// Start Service.
    member this.Start() = Async.RunSync(this.StartAsync())

    /// Start service and block.
    member this.Run() =
        Async.RunSync(async {
            do! this.StartAsync()
            while this.IsRunning do
                do! Async.Sleep 1000
        })

    /// Stop Service and worker loop. Wait for any pending jobs.
    member this.StopAsync () =
        async {
            try
                let sub = subscription |> Option.get
                attachableLogger.LogInfof "Stopping Service %A." serviceId
                sub.Dispose()
                attachableLogger.LogInfof "Service %A stopped." serviceId
            with ex ->
                // TODO : Handle error
                attachableLogger.LogErrorf "Service Stop for %A failed with %A" this.Id ex
                return! Async.Raise ex
        }

    /// Stop Service and worker loop. Wait for any pending jobs.
    member this.Stop () = Async.RunSync(this.StopAsync())