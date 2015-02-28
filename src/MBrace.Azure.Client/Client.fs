namespace MBrace.Azure.Client

    #nowarn "40"
    #nowarn "444"

    open MBrace
    open MBrace.Azure
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
    type Runtime private (clientId, config : Configuration) =
        let configuration = config.WithAppendedId
        do Configuration.AddIgnoredAssembly(typeof<Runtime>.Assembly)
           Async.RunSync(Configuration.ActivateAsync(configuration))
        let state = Async.RunSync(RuntimeState.FromConfiguration(configuration))
        let storageLogger = new StorageLogger(configuration.ConfigurationId, Client(id = clientId))
        let clientLogger = new LoggerCombiner()
        do  clientLogger.Attach(storageLogger)
        let wmon = WorkerManager.Create(configuration.ConfigurationId)
        let resources, defaultStoreClient = StoreClient.CreateDefault(configuration)
        let compiler = CloudCompiler.Init()
        let pmon = state.ProcessMonitor
        do clientLogger.Logf "Client %s created" clientId

        member private __.RuntimeState = state

        /// Provides common methods on store related primitives.
        member __.DefaultStoreClient = defaultStoreClient

        /// Instance identifier.
        member __.ClientId = clientId

        /// Gets the runtime associated configuration.
        member __.Configuration = configuration

        /// Client logger.
        member __.ClientLogger : ICloudLogger = clientLogger :> _
        
        /// Attach given logger to ClientLogger.
        member __.AttachClientLogger(logger : ICloudLogger) = clientLogger.Attach(logger)

        /// <summary>
        /// Creates a fresh CloudCancellationTokenSource for this runtime.
        /// </summary>
        member __.CreateCancellationTokenSource () =
            state.ResourceFactory.RequestCancellationTokenSource(guid()) 
            |> Async.RunSync :> ICloudCancellationTokenSource

        /// <summary>
        /// Execute given workflow locally using thread parallelism and await for its result.
        /// </summary>
        /// <param name="workflow">The workflow to execute.</param>
        /// <param name="logger">Optional logger to use.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <param name="faultPolicy">Optional fault policy.</param>
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

        /// <summary>
        /// Execute given workflow locally using thread parallelism and await for its result.
        /// </summary>
        /// <param name="workflow">The workflow to execute.</param>
        /// <param name="logger">Optional logger to use.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <param name="faultPolicy">Optional fault policy.</param>
        member __.RunLocal(workflow : Cloud<'T>, ?logger : ICloudLogger, ?cancellationToken : CancellationToken, ?faultPolicy : FaultPolicy) : 'T =
            __.RunLocalAsync(workflow, ?logger = logger, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy)
            |> Async.RunSynchronously

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
        member __.CreateProcessAsTask(workflow : Cloud<'T>, ?name : string, ?defaultDirectory : string,?fileStore : ICloudFileStore,?defaultAtomContainer : string,?atomProvider : ICloudAtomProvider,?defaultChannelContainer : string,?channelProvider : ICloudChannelProvider,?cancellationToken : ICloudCancellationToken, ?faultPolicy : FaultPolicy) =
            __.CreateProcessAsync(workflow, ?name = name, ?defaultDirectory = defaultDirectory, ?fileStore = fileStore, ?defaultAtomContainer = defaultAtomContainer, ?atomProvider = atomProvider, ?defaultChannelContainer = defaultChannelContainer, ?channelProvider = channelProvider, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy )
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
        member __.CreateProcess(workflow : Cloud<'T>,  ?name : string,  ?defaultDirectory : string, ?fileStore : ICloudFileStore, ?defaultAtomContainer : string, ?atomProvider : ICloudAtomProvider, ?defaultChannelContainer : string, ?channelProvider : ICloudChannelProvider, ?cancellationToken : ICloudCancellationToken,  ?faultPolicy : FaultPolicy) : Process<'T> =
            __.CreateProcessAsync(workflow, ?name = name, ?defaultDirectory = defaultDirectory, ?fileStore = fileStore, ?defaultAtomContainer = defaultAtomContainer, ?atomProvider = atomProvider, ?defaultChannelContainer = defaultChannelContainer, ?channelProvider = channelProvider, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy )
            |> Async.RunSynchronously

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
        member __.CreateProcessAsync(workflow : Cloud<'T>, 
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
                let faultPolicy = match faultPolicy with Some fp -> fp | None -> FaultPolicy.InfiniteRetry()
                let computation = compiler.Compile(workflow, ?name = name)
          
                let pid = guid ()
                let info = 
                    { 
                        Id = pid
                        Name = defaultArg name computation.Name
                        DefaultDirectory = defaultArg defaultDirectory configuration.UserDataContainer
                        FileStore = fileStore
                        DefaultAtomContainer = defaultArg defaultAtomContainer configuration.UserDataTable
                        AtomProvider = atomProvider
                        DefaultChannelContainer = defaultArg defaultChannelContainer configuration.UserDataContainer
                        ChannelProvider = channelProvider 
                    }

                clientLogger.Logf "Creating process %s %s" info.Id info.Name
                clientLogger.Logf "Calculating dependencies." 
                for d in computation.Dependencies do
                    clientLogger.Logf "- %s" d.FullName
                clientLogger.Logf "Uploading dependencies."
                do! state.AssemblyManager.UploadDependencies(computation.Dependencies)
                clientLogger.Logf "Submit process %s." info.Id
                let! _ = state.StartAsProcess(info, computation.Dependencies, faultPolicy, computation.Workflow, ?ct = cancellationToken)
                clientLogger.Logf "Created process %s." info.Id
                return Process<'T>(configuration.ConfigurationId, info.Id, pmon)
            }
            
        /// <summary>
        ///     Asynchronously execute a workflow on the distributed runtime.
        /// </summary>
        /// <param name="workflow">Workflow to be executed.</param>
        /// <param name="cancellationToken">Cancellation token for computation.</param>
        /// <param name="faultPolicy">Fault policy. Defaults to infinite retries.</param>
        member __.RunAsync(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?faultPolicy) = async {
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
        member __.RunAsTask(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?faultPolicy) =
            let asyncwf = __.RunAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy)
            Async.StartAsTask(asyncwf)

        /// <summary>
        ///     Execute a workflow on the distributed runtime synchronously
        /// </summary>
        /// <param name="workflow">Workflow to be executed.</param>
        /// <param name="cancellationToken">Cancellation token for computation.</param>
        /// <param name="faultPolicy">Fault policy. Defaults to infinite retries.</param>
        member __.Run(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?faultPolicy) =
            __.RunAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy) |> Async.RunSync

        /// <summary>
        /// Get runtime workers.
        /// </summary>
        /// <param name="timespan">Optional timespan. Include workers that failed to give heartbeats in this timespan.</param>
        /// <param name="showInactive">Optionally include inactive workers.</param>
        member __.GetWorkers(?timespan : TimeSpan, ?showInactive : bool) = 
            Async.RunSync <| __.GetWorkersAsync(?timespan = timespan, ?showInactive = showInactive)
        /// <summary>
        /// Get runtime workers.
        /// </summary>
        /// <param name="timespan">Optional timespan. Include workers that failed to give heartbeats in this timespan.</param>
        /// <param name="showInactive">Optionally include inactive workers.</param>
        member __.GetWorkersAsync(?timespan : TimeSpan, ?showInactive : bool) = 
            wmon.GetWorkerRefs(?timespan = timespan, ?showInactive = showInactive)
        /// <summary>
        /// Print runtime workers.
        /// </summary>
        /// <param name="timespan">Optional timespan. Include workers that failed to give heartbeats in this timespan.</param>
        /// <param name="showInactive">Optionally include inactive workers.</param>
        member __.ShowWorkers (?timespan : TimeSpan, ?showInactive : bool) = 
            let ws = wmon.GetWorkers(?timespan = timespan, ?showInactive = showInactive) |> Async.RunSync
            printf "%s" <| WorkerReporter.Report(ws, "Workers", false)

        /// <summary>
        /// Get runtime logs.
        /// </summary>
        /// <param name="worker">Get logs from specific worker.</param>
        /// <param name="fromDate">Get logs from this date.</param>
        /// <param name="toDate">Get logs until this date.</param>
        member __.GetLogs(?worker : IWorkerRef, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) = 
            Async.RunSync <| __.GetLogsAsync(?worker = worker, ?fromDate = fromDate, ?toDate = toDate)
        /// <summary>
        /// Get runtime logs.
        /// </summary>
        /// <param name="worker">Get logs from specific worker.</param>
        /// <param name="fromDate">Get logs from this date.</param>
        /// <param name="toDate">Get logs until this date.</param>
        member __.GetLogsAsync(?worker : IWorkerRef, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) = 
            let loggerType = worker |> Option.map (fun w -> Worker w.Id)
            storageLogger.GetLogs(?loggerType = loggerType, ?fromDate = fromDate, ?toDate = toDate)
        /// <summary>
        /// Print runtime logs.
        /// </summary>
        /// <param name="worker">Get logs from specific worker.</param>
        /// <param name="fromDate">Get logs from this date.</param>
        /// <param name="toDate">Get logs until this date.</param>
        member __.ShowLogs(?worker : IWorkerRef, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) =
            let ls = __.GetLogs(?worker = worker, ?fromDate = fromDate, ?toDate = toDate)
            printf "%s" <| LogReporter.Report(ls, "Logs", false)
        /// <summary>
        /// Print runtime logs.
        /// </summary>
        /// <param name="worker">Get logs from specific worker.</param>
        /// <param name="n">Get logs written the last n seconds.</param>
        member __.ShowLogs(n : float, ?worker : IWorkerRef) =
            let fromDate = DateTimeOffset.Now - (TimeSpan.FromSeconds n)
            let ls = __.GetLogs(?worker = worker, fromDate = fromDate)
            printf "%s" <| LogReporter.Report(ls, "Logs", false)

        /// <summary>
        /// Get a process handle for given process id.
        /// </summary>
        /// <param name="pid">Process Id</param>
        member __.GetProcess(pid) = Async.RunSync <| __.GetProcessAsync(pid)
        /// <summary>
        /// Get a process handle for given process id.
        /// </summary>
        /// <param name="pid">Process Id</param>
        member __.GetProcessAsync(pid) = 
            async {
                let! e = pmon.GetProcess(pid)
                let deps = e.UnpickleDependencies()
                do! state.AssemblyManager.LoadDependencies(deps) // TODO : revise
                return Process.Create(configuration.ConfigurationId, pid, e.UnpickleType(), pmon)
            }
        /// <summary>
        /// Print process information for given process id.
        /// </summary>
        /// <param name="pid">Process Id</param>
        member __.ShowProcess(pid) =
            let ps = __.GetProcess(pid).ProcessEntity.Value
            printf "%s" <| ProcessReporter.Report([ps], "Process", false)

        /// Print all process information.
        member __.ShowProcesses () = 
            let ps = pmon.GetProcesses() |> Async.RunSync
            printf "%s" <| ProcessReporter.Report(ps, "Processes", false)

        /// <summary>
        /// Delete runtime records for given process.
        /// </summary>
        /// <param name="pid">Process Id.</param>
        /// <param name="force">Force deletion on not completed processes.</param>
        member __.ClearProcess(pid, ?force) = __.ClearProcessAsync(pid, ?force = force) |> Async.RunSync
        /// <summary>
        /// Delete runtime records for given process.
        /// </summary>
        /// <param name="pid">Process Id.</param>
        /// <param name="force">Force deletion on not completed processes.</param>
        member __.ClearProcessAsync(pid, ?force : bool) = 
            let force = defaultArg force false
            pmon.ClearProcess(pid, force = force)
        
        /// <summary>
        /// Delete runtime records for all processes.
        /// </summary>
        /// <param name="force">Force deletion on not completed processes.</param>
        member __.ClearAllProcesses(?force) = __.ClearAllProcessesAsync(?force = force) |> Async.RunSync
        /// <summary>
        /// Delete runtime records for all processes.
        /// </summary>
        /// <param name="force">Force deletion on not completed processes.</param>
        member __.ClearAllProcessesAsync(?force : bool) = 
            let force = defaultArg force false
            pmon.ClearAllProcesses(force = force)

        /// <summary>
        /// Delete and re-activate runtime state.
        /// Using 'Reset' may cause unexpected behavior in clients and workers.</summary>
        /// Workers should be restarted manually.
        /// <param name="deleteQueue">Delete runtime queues. Defaults to true.</param>
        /// <param name="deleteState">Delete runtime container and table. Defaults to true.</param>
        /// <param name="deleteLogs">Delete runtime logs table. Defaults to true.</param>
        /// <param name="deleteUserData">Delete Configuration.UserData container and table. Defaults to true.</param>
        /// <param name="reactivate">Reactivate configuration.</param>
        [<CompilerMessage("Using 'Reset' may cause unexpected behavior in clients and workers.", 445)>]
        member __.Reset(?deleteQueue, ?deleteState, ?deleteLogs, ?deleteUserData, ?reactivate) =
            async {
                let deleteState = defaultArg deleteState true
                let deleteQueue = defaultArg deleteQueue true
                let deleteUserData = defaultArg deleteUserData true
                let deleteRuntimeLogs = defaultArg deleteLogs true
                let reactivate = defaultArg reactivate true

                clientLogger.Logf "Calling Reset."
                storageLogger.Stop()
                let cl = new ConsoleLogger() // Using client (storage) logger will throw exc.
                
                clientLogger.Logf "Deleting Queues."
                if deleteQueue then do! Configuration.DeleteRuntimeQueues(configuration)
                clientLogger.Logf "Deleting Container and Table."
                if deleteState then do! Configuration.DeleteRuntimeState(configuration)
                clientLogger.Logf "Deleting Logs."
                if deleteRuntimeLogs then do! Configuration.DeleteRuntimeLogs(configuration)
                clientLogger.Logf "Deleting UserData."
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
                    let! _ = RuntimeState.FromConfiguration(configuration)

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