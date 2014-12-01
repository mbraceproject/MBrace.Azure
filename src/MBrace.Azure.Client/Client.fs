namespace Nessos.MBrace.Azure.Client

    open System.IO
    open System.Threading

    open Nessos.MBrace
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Azure.Runtime
    open Nessos.MBrace.Azure.Runtime.Common
    open Nessos.MBrace.Runtime.Compiler

    #nowarn "40"
    open System

    /// <summary>
    /// Windows Azure Runtime client.
    /// </summary>
    [<AutoSerializable(false)>]
    type Runtime private (config : Configuration) =
        let clientId = guid()
        do Configuration.Activate(config)
        let state = RuntimeState.FromConfiguration(config)
        let logger = new StorageLogger(config.ConfigurationId, config.DefaultLogTable, Client(id = clientId))
        do state.ResourceFactory.Logger.Attach(logger)
        let wmon = state.ResourceFactory.WorkerMonitor
        let pmon = state.ResourceFactory.ProcessMonitor
        do logger.Logf "Client %s created" clientId

        member private __.RuntimeState = state

        /// Instance identifier.
        member __.ClientId = clientId

        /// Gets the runtime associated configuration.
        member __.Configuration = config

        /// Client logger.
        member __.ClientLogger = logger

        member __.CreateProcessAsTask(workflow : Cloud<'T>, ?name : string, ?cancellationToken : CancellationToken) =
            Async.StartAsTask(__.CreateProcessAsync(workflow, ?name = name, ?cancellationToken = cancellationToken))

        member __.CreateProcess(workflow : Cloud<'T>, ?name : string, ?cancellationToken : CancellationToken) : Process<'T> =
            Async.RunSynchronously(__.CreateProcessAsync(workflow, ?name = name, ?cancellationToken = cancellationToken))

        member __.CreateProcessAsync(workflow : Cloud<'T>, ?name : string, ?cancellationToken : CancellationToken) : Async<Process<'T>> =
            async {
                let computation = CloudCompiler.Compile workflow
                let processId = guid()
                let pname = defaultArg name computation.Name
                logger.Logf "Creating process %s %s" processId pname
                let storageId = Storage.processIdToStorageId processId
                logger.Logf "Uploading dependencies %O" computation.Dependencies
                do! state.AssemblyManager.UploadDependencies(computation.Dependencies)
                logger.Logf "Creating DistributedCancellationToken"
                let! cts = state.ResourceFactory.RequestCancellationTokenSource(storageId)
                cancellationToken |> Option.iter (fun ct -> ct.Register(fun () -> cts.Cancel()) |> ignore)
                logger.Logf "Starting process %s" processId
                let! resultCell = state.StartAsProcess(processId, pname, computation.Dependencies, cts, computation.Workflow)
                logger.Logf "Created process %s" processId
                return Process<'T>(config.ConfigurationId, processId, pmon)
            }
            
        /// Asynchronously execute a workflow on the distributed runtime.
        member __.RunAsync(workflow : Cloud<'T>, ?cancellationToken : CancellationToken) = async {
            let! p = __.CreateProcessAsync(workflow, ?cancellationToken = cancellationToken)
            try
                return p.AwaitResult()
            finally
                p.DistributedCancellationTokenSource.Cancel()
        }

        /// Execute a workflow on the distributed runtime as task.
        member __.RunAsTask(workflow : Cloud<'T>, ?cancellationToken : CancellationToken) =
            let asyncwf = __.RunAsync(workflow, ?cancellationToken = cancellationToken)
            Async.StartAsTask(asyncwf)

        /// Execute a workflow on the distributed runtime synchronously
        member __.Run(workflow : Cloud<'T>, ?cancellationToken : CancellationToken) =
            __.RunAsync(workflow, ?cancellationToken = cancellationToken) |> Async.RunSynchronously


        member __.GetWorkers () = Async.RunSynchronously <| __.GetWorkersAsync()
        member __.GetWorkersAsync () = wmon.GetWorkers()
        member __.ShowWorkers () = 
            let ws = __.GetWorkers() 
            printf "%s" <| WorkerReporter.Report(ws, "Workers", false)

        member __.GetLogs () = Async.RunSynchronously <| __.GetLogsAsync()
        member __.GetLogsAsync () = logger.GetLogsAsync()
        member __.ShowLogs () =
            let ls = __.GetLogs()
            printf "%s" <| LogReporter.Report(ls, "Logs", false)

        member __.GetProcess(pid) = Async.RunSynchronously <| __.GetProcessAsync(pid)
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

        member __.GetProcesses () = Async.RunSynchronously <| __.GetProcessesAsync()
        member __.GetProcessesAsync () : Async<seq<Process>> = 
            async {
                let! ps = pmon.GetProcesses()
                let rs = new ResizeArray<Process>()
                for p in ps do
                    let! proc = __.GetProcessAsync(p.Id)
                    rs.Add(proc)
                return rs :> seq<_>
            }
        member __.ShowProcesses () = 
            let ps = __.GetProcesses() |> Seq.map (fun ps -> ps.ProcessEntity.Value)
            printf "%s" <| ProcessReporter.Report(ps, "Processes", true)

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

            Async.RunSynchronously(loop ())
            runtime