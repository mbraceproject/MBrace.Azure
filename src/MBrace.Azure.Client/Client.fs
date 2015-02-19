namespace MBrace.Azure.Client

    #nowarn "40"
    #nowarn "444"

    open MBrace
    open MBrace.Azure.Runtime
    open MBrace.Azure.Runtime.Common
    open MBrace.Continuation
    open MBrace.Runtime
    open MBrace.Runtime.Compiler
    open MBrace.Runtime.InMemory
    open MBrace.Store
    open System
    open System.Threading

    /// <summary>
    /// Windows Azure Runtime client.
    /// </summary>
    [<AutoSerializable(false)>]
    type Runtime private (config : Configuration) =
        let clientId = guid()
        do Configuration.AddIgnoredAssembly(typeof<Runtime>.Assembly)
        do Configuration.Activate(config)
        let state = Async.RunSync(RuntimeState.FromConfiguration(config))
        let storageLogger = new StorageLogger(config.ConfigurationId, config.DefaultLogTable, Client(id = clientId))
        let clientLogger = new LoggerCombiner()
        do  clientLogger.Attach(storageLogger)
        let wmon = WorkerManager.Create(config)
        let resources, defaultStoreClient = StoreClient.CreateDefault(config)
        let compiler = CloudCompiler.Init()
        let pmon = state.ProcessMonitor
        do clientLogger.Logf "Client %s created" clientId

        member private __.RuntimeState = state

        member __.DefaultStoreClient with get () = defaultStoreClient

        /// Instance identifier.
        member __.ClientId = clientId

        /// Gets the runtime associated configuration.
        member __.Configuration = config

        /// Client logger.
        member __.ClientLogger : ICloudLogger = clientLogger :> _
        /// Attach given logger to ClientLogger.
        member __.AttachLogger(logger : ICloudLogger) = clientLogger.Attach(logger)

        /// <summary>
        /// Creates a fresh cloud cancellation token source for this runtime
        /// </summary>
        /// <param name="table">Optional table name where the cancellation token will be created. Defaults to Configuration.DefaultTable.</param>
        member __.CreateCancellationTokenSource (?table : string) =
            let table = defaultArg table config.DefaultTableOrContainer
            state.ResourceFactory.RequestCancellationTokenSource(table) |> Async.RunSync :> ICloudCancellationTokenSource

        member __.RunLocalAsync(workflow : Cloud<'T>, ?logger : ICloudLogger, ?cancellationToken : CancellationToken, ?faultPolicy : FaultPolicy) : Async<'T> =
            async {
                let runtimeProvider = ThreadPoolRuntime.Create(?logger = logger, ?faultPolicy = faultPolicy)
                let rsc = resource { yield! resources; yield runtimeProvider :> ICloudRuntimeProvider }
                let! ct = 
                    match cancellationToken with
                    | Some ct -> async { return ct }
                    | None -> Async.CancellationToken
                return! Cloud.ToAsync(workflow, rsc, new InMemoryCancellationToken(ct))
            }

        member __.RunLocal(workflow : Cloud<'T>, ?logger : ICloudLogger, ?cancellationToken : CancellationToken, ?faultPolicy : FaultPolicy) : 'T =
            __.RunLocalAsync(workflow, ?logger = logger, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy)
            |> Async.RunSynchronously

        member __.CreateProcessAsTask(workflow : Cloud<'T>, ?name : string, ?defaultDirectory : string,?fileStore : ICloudFileStore,?defaultAtomContainer : string,?atomProvider : ICloudAtomProvider,?defaultChannelContainer : string,?channelProvider : ICloudChannelProvider,?cancellationToken : CancellationToken, ?faultPolicy : FaultPolicy) =
            __.CreateProcessAsync(workflow, ?name = name, ?defaultDirectory = defaultDirectory, ?fileStore = fileStore, ?defaultAtomContainer = defaultAtomContainer, ?atomProvider = atomProvider, ?defaultChannelContainer = defaultChannelContainer, ?channelProvider = channelProvider, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy )
            |> Async.StartAsTask

        member __.CreateProcess(workflow : Cloud<'T>,  ?name : string,  ?defaultDirectory : string, ?fileStore : ICloudFileStore, ?defaultAtomContainer : string, ?atomProvider : ICloudAtomProvider, ?defaultChannelContainer : string, ?channelProvider : ICloudChannelProvider, ?cancellationToken : CancellationToken,  ?faultPolicy : FaultPolicy) : Process<'T> =
            __.CreateProcessAsync(workflow, ?name = name, ?defaultDirectory = defaultDirectory, ?fileStore = fileStore, ?defaultAtomContainer = defaultAtomContainer, ?atomProvider = atomProvider, ?defaultChannelContainer = defaultChannelContainer, ?channelProvider = channelProvider, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy )
            |> Async.RunSynchronously

        member __.CreateProcessAsync(workflow : Cloud<'T>, 
                                     ?name : string, 
                                     ?defaultDirectory : string,
                                     ?fileStore : ICloudFileStore,
                                     ?defaultAtomContainer : string,
                                     ?atomProvider : ICloudAtomProvider,
                                     ?defaultChannelContainer : string,
                                     ?channelProvider : ICloudChannelProvider,
                                     ?cancellationToken : CancellationToken, 
                                     ?faultPolicy : FaultPolicy) : Async<Process<'T>> =
            async {
                let faultPolicy = match faultPolicy with Some fp -> fp | None -> FaultPolicy.InfiniteRetry()
                let computation = compiler.Compile(workflow, ?name = name)
          
                let info = 
                    let pid = guid ()
                    let defaultContainer = Storage.processIdToStorageId pid
                    { 
                        Id = pid
                        Name = defaultArg name computation.Name
                        DefaultDirectory = defaultArg defaultDirectory defaultContainer
                        FileStore = fileStore
                        DefaultAtomContainer = defaultArg defaultAtomContainer defaultContainer
                        AtomProvider = atomProvider
                        DefaultChannelContainer = defaultArg defaultChannelContainer defaultContainer
                        ChannelProvider = channelProvider 
                    }

                clientLogger.Logf "Creating process %s %s" info.Id info.Name
                clientLogger.Logf "Calculating dependencies" 
                for d in computation.Dependencies do
                    clientLogger.Logf "%s" d.FullName
                do! state.AssemblyManager.UploadDependencies(computation.Dependencies)
                clientLogger.Logf "Creating DistributedCancellationToken"
                let! cts = state.ResourceFactory.RequestCancellationTokenSource(info.DefaultDirectory)
                cancellationToken |> Option.iter (fun ct -> ct.Register(fun () -> cts.Cancel()) |> ignore)
                clientLogger.Logf "Starting process %s" info.Id
                let! resultCell = state.StartAsProcess(info, computation.Dependencies, cts, faultPolicy, computation.Workflow)
                clientLogger.Logf "Created process %s" info.Id
                return Process<'T>(config.ConfigurationId, info.Id, pmon)
            }
            
        /// <summary>
        ///     Asynchronously execute a workflow on the distributed runtime.
        /// </summary>
        /// <param name="workflow">Workflow to be executed.</param>
        /// <param name="cancellationToken">Cancellation token for computation.</param>
        /// <param name="faultPolicy">Fault policy. Defaults to infinite retries.</param>
        member __.RunAsync(workflow : Cloud<'T>, ?cancellationToken : CancellationToken, ?faultPolicy) = async {
            let! p = __.CreateProcessAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy)
            try
                return p.AwaitResult()
            finally
                p.DistributedCancellationTokenSource.Cancel()
        }

        /// <summary>
        ///     Execute a workflow on the distributed runtime as task.
        /// </summary>
        /// <param name="workflow">Workflow to be executed.</param>
        /// <param name="cancellationToken">Cancellation token for computation.</param>
        /// <param name="faultPolicy">Fault policy. Defaults to infinite retries.</param>
        member __.RunAsTask(workflow : Cloud<'T>, ?cancellationToken : CancellationToken, ?faultPolicy) =
            let asyncwf = __.RunAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy)
            Async.StartAsTask(asyncwf)

        /// <summary>
        ///     Execute a workflow on the distributed runtime synchronously
        /// </summary>
        /// <param name="workflow">Workflow to be executed.</param>
        /// <param name="cancellationToken">Cancellation token for computation.</param>
        /// <param name="faultPolicy">Fault policy. Defaults to infinite retries.</param>
        member __.Run(workflow : Cloud<'T>, ?cancellationToken : CancellationToken, ?faultPolicy) =
            __.RunAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy) |> Async.RunSync


        member __.GetWorkers(?timespan : TimeSpan, ?showInactive : bool) = 
            Async.RunSync <| __.GetWorkersAsync(?timespan = timespan, ?showInactive = showInactive)
        member __.GetWorkersAsync(?timespan : TimeSpan, ?showInactive : bool) = 
            wmon.GetWorkerRefs(?timespan = timespan, ?showInactive = showInactive)
        member __.ShowWorkers (?timespan : TimeSpan, ?showInactive : bool) = 
            let ws = wmon.GetWorkers(?timespan = timespan, ?showInactive = showInactive) |> Async.RunSync
            printf "%s" <| WorkerReporter.Report(ws, "Workers", false)

        member __.GetLogs(?worker : IWorkerRef, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) = 
            Async.RunSync <| __.GetLogsAsync(?worker = worker, ?fromDate = fromDate, ?toDate = toDate)
        member __.GetLogsAsync(?worker : IWorkerRef, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) = 
            let loggerType = worker |> Option.map (fun w -> Worker w.Id)
            storageLogger.GetLogs(?loggerType = loggerType, ?fromDate = fromDate, ?toDate = toDate)
        member __.ShowLogs(?worker : IWorkerRef, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) =
            let ls = __.GetLogs(?worker = worker, ?fromDate = fromDate, ?toDate = toDate)
            printf "%s" <| LogReporter.Report(ls, "Logs", false)

        member __.GetProcess(pid) = Async.RunSync <| __.GetProcessAsync(pid)
        member __.GetProcessAsync(pid) = 
            async {
                let! e = pmon.GetProcess(pid)
                let deps = e.UnpickleDependencies()
                do! state.AssemblyManager.LoadDependencies(deps) // TODO : revise
                return Process.Create(config.ConfigurationId, pid, e.UnpickleType(), pmon)
            }
        member __.ShowProcess(pid) =
            let ps = __.GetProcess(pid).ProcessEntity.Value
            printf "%s" <| ProcessReporter.Report([ps], "Process", false)

        member __.ShowProcesses () = 
            let ps = pmon.GetProcesses() |> Async.RunSync
            printf "%s" <| ProcessReporter.Report(ps, "Processes", false)

        member __.ClearProcess(pid) = __.ClearProcessAsync(pid) |> Async.RunSync
        member __.ClearProcessAsync(pid) = pmon.ClearProcess(pid)
        
        member __.ClearAllProcesses () = __.ClearAllProcessesAsync() |> Async.RunSync
        member __.ClearAllProcessesAsync () = pmon.ClearAllProcesses()

        /// <summary>
        /// Gets a handle for a remote runtime.
        /// </summary>
        /// <param name="config">Runtime configuration.</param>
        /// <param name="waitWorkerCount">Wait until the specified number of workers join the runtime.</param>
        static member GetHandle(config : Configuration, ?waitWorkerCount : int) : Runtime = 
            let waitWorkerCount = defaultArg waitWorkerCount 0
            if waitWorkerCount < 0 then invalidArg "waitWorkerCount" "Must be greater than 0"

            let runtime = new Runtime(config)
            let rec loop () = async {
                let! ws = runtime.GetWorkersAsync()
                if Seq.length ws >= waitWorkerCount then return ()
                else
                    do! Async.Sleep 500
                    return! loop ()
            }

            Async.RunSync(loop ())
            runtime