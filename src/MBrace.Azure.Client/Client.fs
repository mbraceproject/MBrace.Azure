namespace Nessos.MBrace.Azure.Client

    open System.IO
    open System.Threading

    open Nessos.MBrace
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Azure.Runtime
    open Nessos.MBrace.Azure.Runtime.Common
    open Nessos.MBrace.Runtime.Compiler

    #nowarn "40"

    /// MBrace Sample runtime client instance.
    [<AutoSerializable(false)>]
    type Runtime private (config : Configuration) =
        let clientId = guid()
        do Configuration.Activate(config)
        let state = RuntimeState.FromConfiguration(config)
        let logger = new StorageLogger(config.DefaultLogTable, Client(id = clientId))
        do logger.Attach(new ConsoleLogger()) // TODO : move to Client settings        
        do state.ResourceFactory.Logger.Attach(logger)
        let wmon = state.ResourceFactory.WorkerMonitor
        let pmon = state.ResourceFactory.ProcessMonitor
        do logger.Logf "Client %s created" clientId

        member private __.RuntimeState = state

        member __.ClientId = clientId

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
                do! state.AssemblyExporter.UploadDependencies(computation.Dependencies)
                logger.Logf "Creating DistributedCancellationToken"
                let! cts = state.ResourceFactory.RequestCancellationTokenSource(storageId)
                cancellationToken |> Option.iter (fun ct -> ct.Register(fun () -> cts.Cancel()) |> ignore)
                logger.Logf "Starting process"
                let! resultCell = state.StartAsProcess processId pname computation.Dependencies cts computation.Workflow
                logger.Logf "Created process %s" processId
                return Process<'T>(processId, state.ResourceFactory.ProcessMonitor)
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
            let ws = __.GetWorkers() |> List.ofSeq
            printf "%s" <| WorkerReporter.Report(ws, "Workers", true)

        member __.GetLogs () = Async.RunSynchronously <| __.GetLogsAsync()
        member __.GetLogsAsync () = logger.AsyncGetLogs()
        member __.ShowLogs () =
            let ls = __.GetLogs() |> Seq.sortBy (fun l -> l.Timestamp, l.Type) |> List.ofSeq
            printf "%s" <| LogReporter.Report(ls, "Logs", false)

        member __.GetProcess(pid) = Async.RunSynchronously <| __.GetProcessAsync(pid)
        member __.GetProcessAsync(pid) = 
            async {
                let! e = pmon.GetProcess(pid)
                return Process(pid, pmon)
            }
        member __.ShowProcess(pid) =
            let ps = __.GetProcess(pid)
            printf "%s" <| ProcessReporter.Report([ps], "Process", false)


        member __.GetProcesses () = Async.RunSynchronously <| __.GetProcessesAsync()
        member __.GetProcessesAsync () = 
            async {
                let! ps = pmon.GetProcesses()
                return ps |> Seq.map (fun p -> Process(p.Id, pmon))
            }

        member __.ShowProcesses () = 
            let ps = __.GetProcesses() |> Seq.toList
            printf "%s" <| ProcessReporter.Report(ps, "Processes", true)

        static member GetHandle(config : Configuration) = new Runtime(config)

