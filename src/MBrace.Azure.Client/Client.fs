namespace MBrace.Azure.Client

    #nowarn "40"
    #nowarn "444"

    open MBrace
    open MBrace.Azure
    open MBrace.Azure.Runtime
    open MBrace.Azure.Runtime.Info
    open MBrace.Azure.Runtime.Utilities
    open MBrace.Azure.Runtime.Arguments
    open MBrace.Continuation
    open MBrace.Runtime
    open MBrace.Runtime.InMemory
    open MBrace.Store

    open System
    open System.Diagnostics
    open System.IO
    open System.Threading

    /// <summary>
    /// Windows Azure Runtime client.
    /// </summary>
    [<AutoSerializable(false)>]
    type Runtime private (clientId, config : Configuration) =
        static let lockObj = obj()
        static let mutable localWorkerExecutable : string option = None

        let configuration = config.WithAppendedId
        do 
            Configuration.AddIgnoredAssembly(typeof<Runtime>.Assembly)
            Async.RunSync(Configuration.ActivateAsync(configuration))

        let state = 
            let ignoreVersion = true
                //Version.Parse(config.Version) <> typeof<Runtime>.Assembly.GetName().Version
            RuntimeState.FromConfiguration(configuration, ignoreVersionCompatibility = ignoreVersion)
            |> Async.RunSync
        let storageLogger = new StorageLogger(configuration.ConfigurationId, Client(id = clientId))
        let clientLogger = state.Logger
        do  clientLogger.Attach(storageLogger)
        let wmon = WorkerManager.Create(configuration.ConfigurationId, state.Logger)
        let resources, defaultStoreClient = StoreClient.CreateDefault(configuration)
        let pmon = state.ProcessManager
        do clientLogger.Logf "Client %s created" clientId

        let restrictVersion () = ()
//            let clientVersion = typeof<Runtime>.Assembly.GetName().Version
//            let configVersion = Version.Parse(config.Version)
//            if configVersion <> clientVersion then
//                raise(IncompatibleVersionException(
//                        "Only a restricted set of APIs is supported on older runtime versions." +
//                        "Consider using a client with the corresponding version." +
//                        sprintf "Client version '%O', Configuration version '%O'" clientVersion configVersion))


        /// Gets or sets the path for a local standalone worker executable.
        static member LocalWorkerExecutable
            with get () = match localWorkerExecutable with None -> invalidOp "unset executable path." | Some e -> e
            and set path = 
                lock lockObj (fun () ->
                    let path = Path.GetFullPath path
                    if File.Exists path then localWorkerExecutable <- Some path
                    else raise <| FileNotFoundException(path))

        /// Initialize a new local runtime instance with supplied worker count.
        static member SpawnLocal(config, workerCount, ?maxTasks : int) =
            let exe = Runtime.LocalWorkerExecutable
            if workerCount < 1 then invalidArg "workerCount" "must be positive."  
            let cfg = { Arguments.Configuration = config; Arguments.MaxTasks = defaultArg maxTasks Environment.ProcessorCount}
            do Async.RunSync(Configuration.ActivateAsync(config.WithAppendedId))
            let args = Config.ToBase64Pickle cfg
            let psi = new ProcessStartInfo(exe, args)
            psi.WorkingDirectory <- Path.GetDirectoryName exe
            psi.UseShellExecute <- true
            let _ = Array.Parallel.init workerCount (fun _ -> Process.Start psi)
            ()

        /// Initialize a new local runtime instance with supplied worker count and return a handle.
        static member InitLocal(config, workerCount : int, ?maxTasks : int) : Runtime =
            Runtime.SpawnLocal(config, workerCount, ?maxTasks = maxTasks)
            Runtime.GetHandle(config)

        /// Provides common methods on store related primitives.
        member this.StoreClient = restrictVersion(); defaultStoreClient

        /// Instance identifier.
        member this.ClientId = clientId

        /// Gets the runtime associated configuration.
        member this.Configuration = configuration

        /// Client logger.
        member this.ClientLogger : ICloudLogger = clientLogger :> _
        
        /// Attach given logger to ClientLogger.
        member this.AttachClientLogger(logger : ICloudLogger) = clientLogger.Attach(logger)

        /// <summary>
        /// Creates a fresh CloudCancellationTokenSource for this runtime.
        /// </summary>
        member this.CreateCancellationTokenSource () =
            restrictVersion()
            state.ResourceFactory.RequestCancellationTokenSource(guid()) 
            |> Async.RunSync :> ICloudCancellationTokenSource

        /// <summary>
        /// Execute given workflow locally using thread parallelism and await for its result.
        /// </summary>
        /// <param name="workflow">The workflow to execute.</param>
        /// <param name="logger">Optional logger to use.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <param name="faultPolicy">Optional fault policy.</param>
        member this.RunLocalAsync(workflow : Cloud<'T>, ?logger : ICloudLogger, ?cancellationToken : CancellationToken, ?faultPolicy : FaultPolicy) : Async<'T> =
            async {
                restrictVersion()
                let runtimeProvider = ThreadPoolRuntime.Create(?logger = logger, ?faultPolicy = faultPolicy)
                let rsc = resource { yield! resources; yield runtimeProvider :> IDistributionProvider }
                let! ct = 
                    match cancellationToken with
                    | Some ct -> async { return ct }
                    | None -> Async.CancellationToken
                return! Cloud.ToAsync(workflow, rsc, new InMemoryCancellationToken(ct))
            }

        /// <summary>
        /// Execute given workflow locally using thread parallelism and await for its result.
        /// </summary>
        /// <param name="workflow">The workflow to execute.</param>
        /// <param name="logger">Optional logger to use.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <param name="faultPolicy">Optional fault policy.</param>
        member this.RunLocal(workflow : Cloud<'T>, ?logger : ICloudLogger, ?cancellationToken : CancellationToken, ?faultPolicy : FaultPolicy) : 'T =
            this.RunLocalAsync(workflow, ?logger = logger, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy)
            |> Async.RunSync

        /// <summary>
        /// Submit given workflow for execution and get a Process handle.
        /// </summary>
        /// <param name="workflow">The workflow to execute.</param>
        /// <param name="name">Optional process name.</param>
        /// <param name="defaultDirectory">Optional default directory for CloudFileStore operations.</param>
        /// <param name="fileStore">Optional CloudFileStore.</param>
        /// <param name="defaultAtomContainer">Optional default container for CloudAtom operations.</param>
        /// <param name="atomProvider">Optional CloudAtomProvider.</param>
        /// <param name="defaultChannelContainer">Optional default container for CloudChannel operations.</param>
        /// <param name="channelProvider">Optional CloudChannelProvider.</param>
        /// <param name="cancellationToken">Optional CloudCancellationToken.</param>
        /// <param name="faultPolicy">Optional FaultPolicy. Defaults to InfiniteRetry.</param>
        member this.CreateProcessAsTask(workflow : Cloud<'T>, ?name : string, ?defaultDirectory : string,?fileStore : ICloudFileStore,?defaultAtomContainer : string,?atomProvider : ICloudAtomProvider,?defaultChannelContainer : string,?channelProvider : ICloudChannelProvider,?cancellationToken : ICloudCancellationToken, ?faultPolicy : FaultPolicy) =
            this.CreateProcessAsync(workflow, ?name = name, ?defaultDirectory = defaultDirectory, ?fileStore = fileStore, ?defaultAtomContainer = defaultAtomContainer, ?atomProvider = atomProvider, ?defaultChannelContainer = defaultChannelContainer, ?channelProvider = channelProvider, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy )
            |> Async.StartAsTask

        /// <summary>
        /// Submit given workflow for execution and get a Process handle.
        /// </summary>
        /// <param name="workflow">The workflow to execute.</param>
        /// <param name="name">Optional process name.</param>
        /// <param name="defaultDirectory">Optional default directory for CloudFileStore operations.</param>
        /// <param name="fileStore">Optional CloudFileStore.</param>
        /// <param name="defaultAtomContainer">Optional default container for CloudAtom operations.</param>
        /// <param name="atomProvider">Optional CloudAtomProvider.</param>
        /// <param name="defaultChannelContainer">Optional default container for CloudChannel operations.</param>
        /// <param name="channelProvider">Optional CloudChannelProvider.</param>
        /// <param name="cancellationToken">Optional CloudCancellationToken.</param>
        /// <param name="faultPolicy">Optional FaultPolicy. Defaults to InfiniteRetry.</param>
        member this.CreateProcess(workflow : Cloud<'T>,  ?name : string,  ?defaultDirectory : string, ?fileStore : ICloudFileStore, ?defaultAtomContainer : string, ?atomProvider : ICloudAtomProvider, ?defaultChannelContainer : string, ?channelProvider : ICloudChannelProvider, ?cancellationToken : ICloudCancellationToken,  ?faultPolicy : FaultPolicy) : Process<'T> =
            this.CreateProcessAsync(workflow, ?name = name, ?defaultDirectory = defaultDirectory, ?fileStore = fileStore, ?defaultAtomContainer = defaultAtomContainer, ?atomProvider = atomProvider, ?defaultChannelContainer = defaultChannelContainer, ?channelProvider = channelProvider, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy )
            |> Async.RunSync

        /// <summary>
        /// Submit given workflow for execution and get a Process handle.
        /// </summary>
        /// <param name="workflow">The workflow to execute.</param>
        /// <param name="name">Optional process name.</param>
        /// <param name="defaultDirectory">Optional default directory for CloudFileStore operations.</param>
        /// <param name="fileStore">Optional CloudFileStore.</param>
        /// <param name="defaultAtomContainer">Optional default container for CloudAtom operations.</param>
        /// <param name="atomProvider">Optional CloudAtomProvider.</param>
        /// <param name="defaultChannelContainer">Optional default container for CloudChannel operations.</param>
        /// <param name="channelProvider">Optional CloudChannelProvider.</param>
        /// <param name="cancellationToken">Optional CloudCancellationToken.</param>
        /// <param name="faultPolicy">Optional FaultPolicy. Defaults to NoRetry.</param>
        member this.CreateProcessAsync(workflow : Cloud<'T>, 
                                       ?name : string, 
                                       ?defaultDirectory : string,
                                       ?fileStore : ICloudFileStore,
                                       ?defaultAtomContainer : string,
                                       ?atomProvider : ICloudAtomProvider,
                                       ?defaultChannelContainer : string,
                                       ?channelProvider : ICloudChannelProvider,
                                       ?cancellationToken : ICloudCancellationToken, 
                                       ?faultPolicy : FaultPolicy) : Async<Process<'T>> =
            async {
                restrictVersion()
                
                let pid = guid ()
                let name = defaultArg name String.Empty
                
                clientLogger.Logf "Creating process %s %s" pid name
                let faultPolicy = match faultPolicy with Some fp -> fp | None -> FaultPolicy.NoRetry
                clientLogger.Logf "Computing dependencies"
                let dependencies = state.AssemblyManager.ComputeDependencies workflow
          
                let info = 
                    { 
                        Id = pid
                        Name = name
                        DefaultDirectory = defaultDirectory 
                        FileStore = fileStore
                        DefaultAtomContainer = defaultAtomContainer 
                        AtomProvider = atomProvider
                        DefaultChannelContainer = defaultChannelContainer
                        ChannelProvider = channelProvider 
                    }

                let! _ = state.AssemblyManager.UploadDependencies(dependencies)
                clientLogger.Logf "Submit process %s." info.Id
                let! _ = state.StartAsProcess(info, dependencies, faultPolicy, workflow, ?ct = cancellationToken)
                clientLogger.Logf "Created process %s." info.Id
                let ps = Process<'T>(configuration.ConfigurationId, info.Id, pmon)
                ProcessCache.Add(ps)
                return ps
            }
            
        /// <summary>
        ///     Asynchronously execute a workflow on the distributed runtime.
        /// </summary>
        /// <param name="workflow">Workflow to be executed.</param>
        /// <param name="cancellationToken">Cancellation token for computation.</param>
        /// <param name="faultPolicy">Fault policy. Defaults to infinite retries.</param>
        member this.RunAsync(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?faultPolicy) = async {
            let! p = this.CreateProcessAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy)
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
        member this.RunAsTask(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?faultPolicy) =
            let asyncwf = this.RunAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy)
            Async.StartAsTask(asyncwf)

        /// <summary>
        ///     Execute a workflow on the distributed runtime synchronously
        /// </summary>
        /// <param name="workflow">Workflow to be executed.</param>
        /// <param name="cancellationToken">Cancellation token for computation.</param>
        /// <param name="faultPolicy">Fault policy. Defaults to infinite retries.</param>
        member this.Run(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?faultPolicy) =
            this.RunAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy) |> Async.RunSync

        /// <summary>
        /// Get runtime workers.
        /// </summary>
        /// <param name="timespan">Optional timespan. Include workers that failed to give heartbeats in this timespan.</param>
        /// <param name="timespan">Optional timespan. Include workers that failed to give heartbeats in this timespan.</param>
        /// <param name="showStarting">Include uninitialized workers. Defaults to true.</param>
        /// <param name="showInactive">Include inactive workers. Defaults to false.</param>
        /// <param name="showFaulted">Include faulted workers. Defaults to true.</param>
        member this.GetWorkers(?timespan : TimeSpan, ?showStarting : bool, ?showInactive : bool, ?showFaulted : bool) = 
            Async.RunSync <| this.GetWorkersAsync(?timespan = timespan, ?showInactive = showInactive, ?showStarting = showStarting, ?showFaulted = showFaulted)
        
        /// <summary>
        /// Get runtime workers.
        /// </summary>
        /// <param name="timespan">Optional timespan. Include workers that failed to give heartbeats in this timespan.</param>
        /// <param name="showStarting">Include uninitialized workers. Defaults to true.</param>
        /// <param name="showInactive">Include inactive workers. Defaults to false.</param>
        /// <param name="showFaulted">Include faulted workers. Defaults to true.</param>
        member this.GetWorkersAsync(?timespan : TimeSpan, ?showStarting : bool, ?showInactive : bool, ?showFaulted : bool) = 
            let timespan = defaultArg timespan WorkerManager.MaxHeartbeatTimeSpan
            let showInactive = defaultArg showInactive false
            let showStarting = defaultArg showStarting true
            let showFaulted = defaultArg showFaulted true
            wmon.GetWorkerRefs(timespan, showStarting, showInactive, showFaulted)

        /// <summary>
        /// Print runtime workers.
        /// </summary>
        /// <param name="timespan">Optional timespan. Include workers that failed to give heartbeats in this timespan.</param>
        /// <param name="timespan">Optional timespan. Include workers that failed to give heartbeats in this timespan.</param>
        /// <param name="showStarting">Include uninitialized workers. Defaults to true.</param>
        /// <param name="showInactive">Include inactive workers. Defaults to false.</param>
        /// <param name="showFaulted">Include faulted workers. Defaults to true.</param>
        member this.ShowWorkers (?timespan : TimeSpan,  ?showStarting : bool, ?showInactive : bool, ?showFaulted : bool) = 
            let ws = this.GetWorkers(?timespan = timespan, ?showInactive = showInactive, ?showStarting = showStarting, ?showFaulted = showFaulted)
            printf "%s" <| WorkerReporter.Report(ws, "Workers", false)

        /// <summary>
        /// Get runtime logs.
        /// </summary>
        /// <param name="worker">Get logs from specific worker.</param>
        /// <param name="fromDate">Get logs from this date.</param>
        /// <param name="toDate">Get logs until this date.</param>
        member this.GetLogs(?worker : IWorkerRef, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) = 
            Async.RunSync <| this.GetLogsAsync(?worker = worker, ?fromDate = fromDate, ?toDate = toDate)

        /// <summary>
        /// Get runtime logs.
        /// </summary>
        /// <param name="worker">Get logs from specific worker.</param>
        /// <param name="fromDate">Get logs from this date.</param>
        /// <param name="toDate">Get logs until this date.</param>
        member this.GetLogsAsync(?worker : IWorkerRef, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) = 
            let loggerType = worker |> Option.map (fun w -> Worker w.Id)
            storageLogger.GetLogs(?loggerType = loggerType, ?fromDate = fromDate, ?toDate = toDate)

        /// <summary>
        /// Print runtime logs.
        /// </summary>
        /// <param name="worker">Get logs from specific worker.</param>
        /// <param name="fromDate">Get logs from this date.</param>
        /// <param name="toDate">Get logs until this date.</param>
        member this.ShowLogs(?worker : IWorkerRef, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) =
            let ls = this.GetLogs(?worker = worker, ?fromDate = fromDate, ?toDate = toDate)
            printf "%s" <| LogReporter.Report(ls, "Logs", false)

        /// <summary>
        /// Print runtime logs.
        /// </summary>
        /// <param name="worker">Get logs from specific worker.</param>
        /// <param name="n">Get logs written the last n seconds.</param>
        member this.ShowLogs(n : float, ?worker : IWorkerRef) =
            let fromDate = DateTimeOffset.Now - (TimeSpan.FromSeconds n)
            let ls = this.GetLogs(?worker = worker, fromDate = fromDate)
            printf "%s" <| LogReporter.Report(ls, "Logs", false)

        /// <summary>
        ///     Attaches a local worker to the cluster instance.
        /// </summary>
        /// <param name="workerCount">Local workers to be spawned. Defaults to 1.</param>
        /// <param name="maxTasks">Maximum tasks for worker. Defaults to local core count.</param>
        member this.AttachLocalWorker(?workerCount:int, ?maxTasks:int) : unit =
            restrictVersion()
            let workerCount = defaultArg workerCount 1
            Runtime.SpawnLocal(config, workerCount, ?maxTasks = maxTasks)

        /// Get handles for all processes.
        member this.GetProcesses() = Async.RunSync <| this.GetProcessesAsync()

        /// Get handles for all processes.
        member this.GetProcessesAsync() =
            async {
                restrictVersion()
                let! ps = pmon.GetProcesses()
                return! ps |> Seq.map (fun p -> this.GetProcessAsync p.Id) |> Async.Parallel
            }

        /// <summary>
        /// Get a process handle for given process id.
        /// </summary>
        /// <param name="pid">Process Id</param>
        member this.GetProcess(pid) = Async.RunSync <| this.GetProcessAsync(pid)

        /// <summary>
        /// Get a process handle for given process id.
        /// </summary>
        /// <param name="pid">Process Id</param>
        member this.GetProcessAsync(pid) = 
            async {
                restrictVersion()
                match ProcessCache.TryGet(pid) with
                | Some ps -> return ps
                | None ->
                    let! e = pmon.GetProcess(pid)
                    let deps = e.UnpickleDependencies()
                    let! localAssemblies = state.AssemblyManager.DownloadDependencies deps
                    let _ = state.AssemblyManager.LoadAssemblies localAssemblies
                    let ps = Process.Create(configuration.ConfigurationId, pid, e.UnpickleType(), pmon)
                    ProcessCache.Add(ps)
                    return ps
            }

        /// <summary>
        /// Print process information for given process id.
        /// </summary>
        /// <param name="pid">Process Id</param>
        member this.ShowProcess(pid) =
            let ps = this.GetProcess(pid).ProcessEntity.Value
            printf "%s" <| ProcessReporter.Report([ps], "Process", false)

        /// Print all process information.
        member this.ShowProcesses () = 
            let ps = pmon.GetProcesses() |> Async.RunSync
            printf "%s" <| ProcessReporter.Report(ps, "Processes", false)

        /// <summary>
        /// Delete runtime records for given process.
        /// </summary>
        /// <param name="pid">Process Id.</param>
        /// <param name="fullClear">Delete all records and blobs used by this process. Defaults to true.</param>
        /// <param name="force">Force deletion on not completed processes.</param>
        member this.ClearProcess(pid, ?fullClear, ?force) = this.ClearProcessAsync(pid, ?fullClear = fullClear, ?force = force) |> Async.RunSync

        /// <summary>
        /// Delete runtime records for given process.
        /// </summary>
        /// <param name="pid">Process Id.</param>
        /// <param name="fullClear">Delete all records and blobs used by this process. Defaults to true.</param>
        /// <param name="force">Force deletion on not completed processes.</param>
        member this.ClearProcessAsync(pid, ?fullClear, ?force : bool) = 
            let force = defaultArg force false
            let fullClear = defaultArg fullClear true
            pmon.ClearProcess(pid, full = fullClear, force = force)
        
        /// <summary>
        /// Delete runtime records for all processes.
        /// </summary>
        /// <param name="fullClear">Delete all records and blobs used by this process.Defaults to true.</param>
        /// <param name="force">Force deletion on not completed processes.</param>
        member this.ClearAllProcesses(?fullClear, ?force) = this.ClearAllProcessesAsync(?fullClear = fullClear, ?force = force) |> Async.RunSync

        /// <summary>
        /// Delete runtime records for all processes.
        /// </summary>
        /// <param name="fullClear">Delete all records and blobs used by this process.Defaults to true.</param>
        /// <param name="force">Force deletion on not completed processes.</param>
        member this.ClearAllProcessesAsync(?fullClear, ?force : bool) = 
            let force = defaultArg force false
            let fullClear = defaultArg fullClear true
            pmon.ClearAllProcesses(force = force, full = fullClear)

        /// <summary>
        /// Delete and re-activate runtime state.
        /// Using 'Reset' may cause unexpected behavior in clients and workers.
        /// Workers should be restarted manually.</summary>
        /// <param name="deleteQueue">Delete runtime queues. Defaults to true.</param>
        /// <param name="deleteState">Delete runtime container and table. Defaults to true.</param>
        /// <param name="deleteLogs">Delete runtime logs table. Defaults to true.</param>
        /// <param name="deleteUserData">Delete Configuration.UserData container and table. Defaults to true.</param>
        /// <param name="reactivate">Reactivate configuration. Defaults to true.</param>
        [<CompilerMessage("Using 'Reset' may cause unexpected behavior in clients and workers.", 445)>]
        member this.Reset(?deleteQueue, ?deleteState, ?deleteLogs, ?deleteUserData, ?reactivate) =
            async {
                let deleteState = defaultArg deleteState true
                let deleteQueue = defaultArg deleteQueue true
                let deleteUserData = defaultArg deleteUserData true
                let deleteRuntimeLogs = defaultArg deleteLogs true
                let reactivate = defaultArg reactivate true

                clientLogger.Logf "Calling Reset."
                storageLogger.Stop()
                let cl = new ConsoleLogger() // Using client (storage) logger will throw exc.
                
                cl.Logf "Deleting Queues."
                if deleteQueue then do! Configuration.DeleteRuntimeQueues(configuration)
                cl.Logf "Deleting Container and Table."
                if deleteState then do! Configuration.DeleteRuntimeState(configuration)
                cl.Logf "Deleting Logs."
                if deleteRuntimeLogs then do! Configuration.DeleteRuntimeLogs(configuration)
                cl.Logf "Deleting UserData."
                if deleteUserData then do! Configuration.DeleteUserData(configuration)
               
                if reactivate then
                    cl.Logf "Activating Configuration."
                    let rec loop retryCount = async {
                        cl.Logf "RetryCount %d." retryCount
                        let! step2 = Async.Catch <| Configuration.ActivateAsync(configuration)
                        match step2 with
                        | Choice1Of2 _ -> 
                            cl.Logf "Done."
                        | Choice2Of2 ex ->
                            cl.Logf "Failed with %A" ex
                            cl.Logf "Waiting."
                            do! Async.Sleep 10000
                            return! loop (retryCount + 1)
                    }
                    do! loop 0

                    cl.Logf "Initializing RuntimeState."
                    let! _ = RuntimeState.FromConfiguration(configuration, ignoreVersionCompatibility = false)

                    storageLogger.Start()

            } |> Async.RunSync

        /// <summary>
        /// Gets a handle for a remote runtime.
        /// </summary>
        /// <param name="config">Runtime configuration.</param>
        /// <param name="waitWorkerCount">Wait until the specified number of workers join the runtime.</param>
        static member GetHandle(config : Configuration, ?waitWorkerCount : int) : Runtime = 
            let waitWorkerCount = defaultArg waitWorkerCount 0
            if waitWorkerCount < 0 then invalidArg "waitWorkerCount" "Must be greater than 0"
            let clientId = guid()
            let runtime = new Runtime(clientId, config)
            let rec loop () = async {
                let! ws = runtime.GetWorkersAsync()
                if Seq.length ws >= waitWorkerCount then return ()
                else
                    do! Async.Sleep 500
                    return! loop ()
            }

            Async.RunSync(loop ())
            runtime

        /// <summary>
        ///     Registers a native assembly dependency to client state.
        /// </summary>
        /// <param name="assemblyPath">Path to native assembly.</param>
        [<CompilerMessage("Native dependency support is an experimental MBrace feature.", 1571)>]
        static member RegisterNativeDependency(assemblyPath : string) =
            ignore <| MBrace.Azure.Runtime.Primitives.BlobAssemblyManager.RegisterNativeDependency assemblyPath

        /// Gets native assembly dependencies registered to client state.
        static member NativeDependencies =
            MBrace.Azure.Runtime.Primitives.BlobAssemblyManager.NativeDependencies