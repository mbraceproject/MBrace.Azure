namespace MBrace.Azure

#nowarn "40"
#nowarn "444"
#nowarn "445"

open System
open System.Diagnostics
open System.IO
open System.Threading

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Internals.InMemoryRuntime
open MBrace.Store
open MBrace.Store.Internals
open MBrace.Azure
open MBrace.Azure.Store
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities
open MBrace.Azure.Runtime.Arguments
open MBrace.Client
open MBrace.Runtime.Utils
open MBrace.Runtime

/// <summary>
/// Windows Azure Runtime client.
/// </summary>
[<AutoSerializable(false)>]
type MBraceAzure private (manager : RuntimeManager, defaultLogger : StorageSystemLogger) =
    inherit MBraceClient(manager)

    static let lockObj = obj()
    static let mutable localWorkerExecutable : string option = None


    /// <summary>
    /// Get runtime logs.
    /// </summary>
    /// <param name="worker">Get logs from specific worker.</param>
    /// <param name="fromDate">Get logs from this date.</param>
    /// <param name="toDate">Get logs until this date.</param>
    member this.GetSystemLogs(?worker : IWorkerRef, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) = 
        let loggerType = worker |> Option.map (fun w -> System w.Id)
        defaultLogger.GetLogs(?loggerType = loggerType, ?fromDate = fromDate, ?toDate = toDate)

    /// <summary>
    /// Print runtime logs.
    /// </summary>
    /// <param name="worker">Get logs from specific worker.</param>
    /// <param name="fromDate">Get logs from this date.</param>
    /// <param name="toDate">Get logs until this date.</param>
    member this.ShowSystemLogs(?worker : IWorkerRef, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) =
        let loggerType = worker |> Option.map (fun w -> System w.Id)
        defaultLogger.ShowLogs(?loggerType = loggerType, ?fromDate = fromDate, ?toDate = toDate)

    /// <summary>
    /// Print runtime logs.
    /// </summary>
    /// <param name="worker">Get logs from specific worker.</param>
    /// <param name="n">Get logs written the last n seconds.</param>
    member this.ShowSystemLogs(n : float, ?worker : IWorkerRef) =
        let fromDate = DateTimeOffset.Now - (TimeSpan.FromSeconds n)
        this.ShowSystemLogs(?worker = worker, fromDate = fromDate)

    /// <summary>
    /// Kill all local worker processes.
    /// </summary>
    member this.KillLocalWorker() =
        let workers = this.Workers
        let exists id hostname =
            if Net.Dns.GetHostName() = hostname then
                let ps = try Some <| Process.GetProcessById(id) with :? ArgumentException -> None
                ps.IsSome
            else false
        workers |> Seq.filter (fun w -> exists w.ProcessId w.Hostname)    
                |> Seq.iter this.KillLocalWorker

    /// <summary>
    /// Kill local worker process.
    /// </summary>
    /// <param name="worker">Local worker to kill.</param>
    member this.KillLocalWorker(worker : WorkerRef) =
        let hostname = System.Net.Dns.GetHostName()
        if worker.Hostname <> hostname then
            failwith "WorkerRef hostname %A does not match local hostname %A" worker.Hostname hostname
        else
            let ps = try Some <| Process.GetProcessById(worker.ProcessId) with :? ArgumentException -> None
            match ps with
            | Some p ->
                (manager :> IRuntimeManager).WorkerManager.DeclareWorkerStatus(worker.WorkerId, WorkerJobExecutionStatus.Stopped)
                |> Async.RunSync
                p.Kill()
            | _ ->
                failwithf "No local process with Id = %d found." worker.ProcessId

    /// Gets or sets the path for a local standalone worker executable.
    static member LocalWorkerExecutable
        with get () = match localWorkerExecutable with None -> invalidOp "unset executable path." | Some e -> e
        and set path = 
            lock lockObj (fun () ->
                let path = Path.GetFullPath path
                if File.Exists path then localWorkerExecutable <- Some path
                else raise <| FileNotFoundException(path))

    /// Initialize a new local runtime instance with supplied worker count.
    static member SpawnLocal(config, workerCount, ?maxTasks : int, ?workerNameF : int -> string) =
        let exe = MBraceAzure.LocalWorkerExecutable
        if workerCount < 1 then invalidArg "workerCount" "must be positive."  
        let cfg = { Arguments.Configuration = config; Arguments.MaxTasks = defaultArg maxTasks Environment.ProcessorCount; Name = None}
        do Async.RunSync(Config.ActivateAsync(config.WithAppendedId, true))
        let _ = Array.Parallel.init workerCount (fun i -> 
            let conf =
                match workerNameF with
                | None -> cfg
                | Some f -> {cfg with Name = Some(f(i)) }
            let args = Config.ToBase64Pickle conf
            let psi = new ProcessStartInfo(exe, args)
            psi.WorkingDirectory <- Path.GetDirectoryName exe
            psi.UseShellExecute <- true
            Process.Start psi)
        ()

    /// Initialize a new local runtime instance with supplied worker count and return a handle.
    static member InitLocal(config, workerCount : int, ?maxTasks : int, ?workerNameF) : MBraceAzure =
        MBraceAzure.SpawnLocal(config, workerCount, ?maxTasks = maxTasks, ?workerNameF = workerNameF)
        MBraceAzure.GetHandle(config)


//    /// <summary>
//    /// Delete and re-activate runtime state.
//    /// Using 'Reset' may cause unexpected behavior in clients and workers.
//    /// Workers should be restarted manually.</summary>
//    /// <param name="deleteQueue">Delete runtime queues. Defaults to true.</param>
//    /// <param name="deleteState">Delete runtime container and table. Defaults to true.</param>
//    /// <param name="deleteLogs">Delete runtime logs table. Defaults to true.</param>
//    /// <param name="deleteUserData">Delete Configuration.UserData container and table. Defaults to true.</param>
//    /// <param name="reactivate">Reactivate configuration. Defaults to true.</param>
//    [<CompilerMessage("Using 'Reset' may cause unexpected behavior in clients and workers.", 445)>]
//    member this.Reset(?deleteQueue, ?deleteState, ?deleteLogs, ?deleteUserData, ?reactivate) =
//        async {
//            let deleteState = defaultArg deleteState true
//            let deleteQueue = defaultArg deleteQueue true
//            let deleteUserData = defaultArg deleteUserData true
//            let deleteLogs = defaultArg deleteLogs true
//            let reactivate = defaultArg reactivate true
//            MBraceAzure.Reset(manager., deleteQueue = deleteQueue, deleteState = deleteState, deleteLogs = deleteLogs, deleteUserData = deleteUserData, reactivate = reactivate)
//        } |> Async.RunSync

    /// <summary>
    /// Delete and re-activate runtime state.
    /// Using 'Reset' may cause unexpected behavior in clients and workers.
    /// Workers should be restarted manually.</summary>
    /// <param name="Configuration">Runtime configuration.</param>
    /// <param name="deleteQueue">Delete runtime queues. Defaults to true.</param>
    /// <param name="deleteState">Delete runtime container and table. Defaults to true.</param>
    /// <param name="deleteLogs">Delete runtime logs table. Defaults to true.</param>
    /// <param name="deleteUserData">Delete Configuration.UserData container and table. Defaults to true.</param>
    /// <param name="reactivate">Reactivate configuration. Defaults to true.</param>
    [<CompilerMessage("Using 'Reset' may cause unexpected behavior in clients and workers.", 445)>]
    static member Reset(configuration : Configuration, ?deleteQueue, ?deleteState, ?deleteLogs, ?deleteUserData, ?reactivate) =
        async {
            let deleteState = defaultArg deleteState true
            let deleteQueue = defaultArg deleteQueue true
            let deleteUserData = defaultArg deleteUserData true
            let deleteRuntimeLogs = defaultArg deleteLogs true
            let reactivate = defaultArg reactivate true

            let cl = new ConsoleLogger() // Using client (storage) logger will throw exc.
            cl.LogInfo "Activating configuration"
            let configuration = configuration.WithAppendedId
            Async.RunSync(Config.ActivateAsync(configuration, true))
             
            cl.LogInfo "Deleting Queues."
            if deleteQueue then do! Config.DeleteRuntimeQueues(configuration)
            cl.LogInfo "Deleting Container and Table."
            if deleteState then do! Config.DeleteRuntimeState(configuration)
            cl.LogInfo "Deleting Logs."
            if deleteRuntimeLogs then do! Config.DeleteRuntimeLogs(configuration)
            cl.LogInfo "Deleting UserData."
            if deleteUserData then do! Config.DeleteUserData(configuration)
               
            if reactivate then
                cl.LogInfo "Activating Configuration."
                let rec loop retryCount = async {
                    cl.LogInfof "RetryCount %d." retryCount
                    let! step2 = Async.Catch <| Config.ActivateAsync(configuration, true)
                    match step2 with
                    | Choice1Of2 _ -> 
                        cl.LogInfo "Done."
                    | Choice2Of2 ex ->
                        cl.LogInfof "Failed with %A" ex
                        cl.LogInfo "Waiting."
                        do! Async.Sleep 10000
                        return! loop (retryCount + 1)
                }
                do! loop 0

                //cl.LogInfo "Initializing RuntimeState."
                //let! _ = RuntimeManager.CreateForClient(config,)
                return ()
        } |> Async.RunSync

    /// <summary>
    /// Gets a handle for a remote runtime.
    /// </summary>
    /// <param name="config">Runtime configuration.</param>
    static member GetHandle(config : Configuration) : MBraceAzure = 
        let hostProc = Diagnostics.Process.GetCurrentProcess()
        let clientId = sprintf "%s-%s-%05d" (System.Net.Dns.GetHostName()) hostProc.ProcessName hostProc.Id
        // TODO : Add Configuration check
        let logger = new AttacheableLogger()
        let storageLogger = StorageSystemLogger.Create(config.StorageConnectionString, config.WithAppendedId.RuntimeLogsTable, clientId)
        let _ = logger.AttachLogger(storageLogger)
        let _ = logger.AttachLogger(ConsoleLogger(true) :> ISystemLogger)
        
        let manager = RuntimeManager.CreateForClient(config, clientId, logger, ResourceRegistry.Empty)
        let runtime = new MBraceAzure(manager, storageLogger)
        runtime