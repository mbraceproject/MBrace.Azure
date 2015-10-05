namespace MBrace.Azure

#nowarn "40"
#nowarn "444"
#nowarn "445"

open System
open System.Diagnostics
open System.IO
open System.Net

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Arguments

/// A system logger that writes entries to stdout
type ConsoleLogger = MBrace.Runtime.ConsoleLogger
/// Struct that specifies a single system log entry
type SystemLogEntry = MBrace.Runtime.SystemLogEntry
/// Struct that specifies a single cloud log entry
type CloudLogEntry = MBrace.Runtime.CloudLogEntry
/// Log level used by the MBrace runtime implementation
type LogLevel = MBrace.Runtime.LogLevel
/// A Serializable object used to identify a specific worker in a cluster.
/// Can be used to point computations for execution at specific machines.
type WorkerRef = MBrace.Runtime.WorkerRef
/// Represents a distributed computation that is being executed by an MBrace runtime
type CloudProcess = MBrace.Runtime.CloudProcess
/// Represents a distributed computation that is being executed by an MBrace runtime
type CloudProcess<'T> = MBrace.Runtime.CloudProcess<'T>

[<AutoSerializable(false); AbstractClass; Sealed>]
type AzureWorker private () =
    
    static let mutable localWorkerExecutable : string option = None

    /// Gets or sets the path for a local standalone worker executable.
    static member LocalExecutable
        with get () = match localWorkerExecutable with None -> invalidOp "unset executable path." | Some e -> e
        and set path = 
            let path = Path.GetFullPath path
            if File.Exists path then localWorkerExecutable <- Some path
            else raise <| FileNotFoundException(path)

    /// <summary>
    ///     Initialize a new local runtime instance with supplied worker count and return a handle.
    /// </summary>
    /// <param name="config">Azure runtime configuration.</param>
    /// <param name="workerId">Unique worker identifier in the cluster.</param>
    /// <param name="workingDirectory">Local working directory for the worker process.</param>
    /// <param name="maxWorkItems">Maximum number of concurrent jobs per worker.</param>
    /// <param name="logLevel">Client and local worker logger verbosity level.</param>
    /// <param name="logFile">Specify local path to system logfile for worker process.</param>
    /// <param name="heartbeatInterval">Heartbeat send interval used by worker.</param>
    /// <param name="heartbeatThreshold">Maximum heartbeat threshold after which a worker is to be declared dead.</param>
    /// <param name="background">Run as background instead of windowed process. Defaults to false.</param>
    static member Spawn(?config : Configuration, ?workerId : string, ?workingDirectory : string, ?maxWorkItems : int, ?logLevel : LogLevel, 
                            ?logFile : string, ?heartbeatInterval : TimeSpan, ?heartbeatThreshold : TimeSpan, ?background : bool) : Process =
        let background = defaultArg background false
        let exe = AzureWorker.LocalExecutable
        let cli = ArgumentConfiguration.Create(?config = config, ?workingDirectory = workingDirectory, ?maxWorkItems = maxWorkItems, ?logLevel = logLevel, 
                                                ?logfile = logFile, ?workerId = workerId, ?heartbeatInterval = heartbeatInterval, ?heartbeatThreshold = heartbeatThreshold)

        let args = ArgumentConfiguration.ToCommandLineArguments(cli)
        let psi = new ProcessStartInfo(exe, args)
        psi.WorkingDirectory <- Path.GetDirectoryName exe
        if background then
            psi.UseShellExecute <- false
            psi.CreateNoWindow <- true
        else
            psi.UseShellExecute <- true

        Process.Start psi

    /// <summary>
    ///     Initialize a new local runtime instance with supplied worker count and return a handle.
    /// </summary>
    /// <param name="workerCount">Number of local workers to spawn.</param>
    /// <param name="config">Azure runtime configuration.</param>
    /// <param name="maxWorkItems">Maximum number of concurrent jobs per worker.</param>
    /// <param name="logLevel">Client and local worker logger verbosity level.</param>
    /// <param name="heartbeatInterval">Heartbeat send interval used by worker.</param>
    /// <param name="heartbeatThreshold">Maximum heartbeat threshold after which a worker is to be declared dead.</param>
    /// <param name="background">Run as background instead of windowed process. Defaults to false.</param>
    static member SpawnMultiple(workerCount : int, ?config : Configuration, ?maxWorkItems : int, ?logLevel : LogLevel, 
                                    ?heartbeatInterval : TimeSpan, ?heartbeatThreshold : TimeSpan, ?background : bool) : Process [] =

        let _ = AzureWorker.LocalExecutable // force early exception
        if workerCount < 1 then invalidArg "workerCount" "must be positive."
        let spawn _ = AzureWorker.Spawn(?config = config, ?maxWorkItems = maxWorkItems, ?logLevel = logLevel, 
                                            ?heartbeatInterval = heartbeatInterval, ?heartbeatThreshold = heartbeatThreshold, ?background = background)

        [|1 .. workerCount|] |> Array.Parallel.map spawn

/// <summary>
///     Windows Azure Cluster management client. Provides methods for management, execution and debugging of MBrace processes in Azure.
/// </summary>
[<AutoSerializable(false)>]
type AzureCluster private (manager : ClusterManager) =
    inherit MBraceClient(manager)

    static do ProcessConfiguration.InitAsClient()

    /// <summary>
    ///     Kill worker process if running on local machine.
    ///     Returns true if successful, false if local worker not found.
    /// </summary>
    /// <param name="worker">Local worker to kill.</param>
    member this.KillLocalWorker(worker : IWorkerRef) : bool =
        let worker = worker :?> WorkerRef
        let hostname = Dns.GetHostName()
        if worker.Hostname <> hostname then false
        else
            try let p = Process.GetProcessById(worker.ProcessId) in p.Kill() ; true
            with :? ArgumentException -> false

    /// <summary>
    ///     Kills all worker processes of the cluster that are running on the local machine.
    /// </summary>
    member this.KillAllLocalWorkers() : unit = this.Workers |> Seq.iter (ignore << this.KillLocalWorker)

    /// <summary>
    /// Delete and re-activate runtime state.
    /// Using 'Reset' may cause unexpected behavior in clients and workers.
    /// Workers should be restarted manually.</summary>
    /// <param name="deleteQueues">Delete Configuration queue and topic. Defaults to true.</param>
    /// <param name="deleteRuntimeState">Delete Configuration table and containers. Defaults to true.</param>
    /// <param name="deleteLogs">Delete Configuration logs table. Defaults to true.</param>
    /// <param name="deleteUserData">Delete Configuration UserData table and container. Defaults to false.</param>
    /// <param name="deleteAssemblyData">Delete assembly data container from blob store. Defaults to false.</param>
    /// <param name="force">Ignore active workers. Defaults to false.</param>
    /// <param name="reactivate">Reactivate configuration. Defaults to true.</param>
    [<CompilerMessage("Using 'Reset' may cause unexpected behavior in clients and workers.", 445)>]
    member this.ResetAsync(?deleteQueues : bool, ?deleteRuntimeState : bool, ?deleteLogs : bool, ?deleteUserData : bool, 
                                ?deleteAssemblyData : bool, ?force : bool, ?reactivate : bool) : Async<unit> =

        manager.ResetCluster(?deleteQueues = deleteQueues, ?deleteRuntimeState = deleteRuntimeState, ?deleteLogs = deleteLogs, 
                                ?deleteUserData = deleteUserData, ?deleteAssemblyData = deleteAssemblyData, 
                                ?force = force, ?reactivate = reactivate)

    /// <summary>
    /// Delete and re-activate runtime state.
    /// Using 'Reset' may cause unexpected behavior in clients and workers.
    /// Workers should be restarted manually.</summary>
    /// <param name="deleteQueues">Delete Configuration queue and topic. Defaults to true.</param>
    /// <param name="deleteRuntimeState">Delete Configuration table and containers. Defaults to true.</param>
    /// <param name="deleteLogs">Delete Configuration logs table. Defaults to true.</param>
    /// <param name="deleteUserData">Delete Configuration UserData table and container. Defaults to false.</param>
    /// <param name="deleteAssemblyData">Delete assembly data container from blob store. Defaults to false.</param>
    /// <param name="force">Ignore active workers. Defaults to false.</param>
    /// <param name="reactivate">Reactivate configuration. Defaults to true.</param>
    [<CompilerMessage("Using 'Reset' may cause unexpected behavior in clients and workers.", 445)>]
    member this.Reset(?deleteQueues : bool, ?deleteRuntimeState : bool, ?deleteLogs : bool, ?deleteUserData : bool, 
                                ?deleteAssemblyData : bool, ?force : bool, ?reactivate : bool) : unit =

        this.ResetAsync(?deleteQueues = deleteQueues, ?deleteRuntimeState = deleteRuntimeState, ?deleteLogs = deleteLogs, 
                                ?deleteUserData = deleteUserData, ?deleteAssemblyData = deleteAssemblyData, 
                                ?force = force, ?reactivate = reactivate)
        |> Async.RunSync


    /// <summary>
    ///     Connects to an MBrace-on-Azure cluster as identified by provided configuration object.
    ///     If successful returns a management handle object to the cluster.
    /// </summary>
    /// <param name="config">Runtime configuration.</param>
    /// <param name="clientId">MBrace.Azure client instance identifier.</param>
    /// <param name="clientId">Custom client id for this instance.</param>
    /// <param name="logger">Custom logger to attach in client.</param>
    /// <param name="logLevel">Logger verbosity level.</param>
    static member ConnectAsync (config : Configuration, ?clientId : string, ?logger : ISystemLogger, ?logLevel : LogLevel) : Async<AzureCluster> = async {
        let hostProc = Diagnostics.Process.GetCurrentProcess()
        let clientId = defaultArg clientId <| sprintf "%s-%s-%05d" (System.Net.Dns.GetHostName()) hostProc.ProcessName hostProc.Id
        let! manager = ClusterManager.Create(config, ?systemLogger = logger)
        let! storageLogger = manager.SystemLoggerManager.CreateLogWriter(clientId)
        let _ = manager.LocalLoggerManager.AttachLogger(storageLogger)
        logLevel |> Option.iter (fun l -> manager.LocalLoggerManager.LogLevel <- l)
        return new AzureCluster(manager)
    }

    /// <summary>
    ///     Connects to an MBrace-on-Azure cluster as identified by provided configuration object.
    ///     If successful returns a management handle object to the cluster.
    /// </summary>
    /// <param name="config">Runtime configuration.</param>
    /// <param name="clientId">MBrace.Azure client instance identifier.</param>
    /// <param name="logger">Custom logger to attach in client.</param>
    /// <param name="logLevel">Logger verbosity level.</param>
    static member Connect (config : Configuration, ?clientId : string, ?logger : ISystemLogger, ?logLevel : LogLevel) : AzureCluster =
        AzureCluster.ConnectAsync(config, ?clientId = clientId, ?logger = logger, ?logLevel = logLevel)
        |> Async.RunSync

    /// <summary>
    ///     Connects to an MBrace-on-Azure cluster as identified by provided store and service bus connection strings.
    ///     If successful returns a management handle object to the cluster.
    /// </summary>
    /// <param name="storageConnectionString">Azure Storage connection string.</param>
    /// <param name="serviceBusConnectionString">Azure Service Bus connection string.</param>
    /// <param name="clientId">MBrace.Azure client instance identifier.</param>
    /// <param name="logger">Custom logger to attach in client.</param>
    /// <param name="logLevel">Logger verbosity level.</param>
    static member Connect(storageConnectionString : string, serviceBusConnectionString : string,  ?clientId : string, ?logger : ISystemLogger, ?logLevel : LogLevel) : AzureCluster = 
        AzureCluster.Connect(new Configuration(storageConnectionString, serviceBusConnectionString), ?clientId = clientId, ?logger = logger, ?logLevel = logLevel)

    /// <summary>
    ///     Spawns a worker instance in the local machine, subscribed to the current cluster configuration.
    /// </summary>
    /// <param name="workerId">Unique worker identifier in the cluster.</param>
    /// <param name="workingDirectory">Local working directory for the worker process.</param>
    /// <param name="maxWorkItems">Maximum number of concurrent jobs per worker.</param>
    /// <param name="logFile">Specify local path to system logfile for worker process.</param>
    /// <param name="logLevel">Client and local worker logger verbosity level.</param>
    /// <param name="heartbeatInterval">Heartbeat send interval used by worker.</param>
    /// <param name="heartbeatThreshold">Maximum heartbeat threshold after which a worker is to be declared dead.</param>
    /// <param name="background">Run as background instead of windowed process. Defaults to false.</param>
    member this.AttachLocalWorker(?workerId : string, ?workingDirectory : string, ?maxWorkItems : int, ?logFile : string, ?logLevel:LogLevel,
                                    ?heartbeatInterval : TimeSpan, ?heartbeatThreshold : TimeSpan, ?background : bool) : unit =
        ignore <| AzureWorker.Spawn(manager.Configuration, ?workerId = workerId, ?workingDirectory = workingDirectory, ?maxWorkItems = maxWorkItems, ?logFile = logFile, 
                            ?logLevel = logLevel, ?heartbeatInterval = heartbeatInterval, ?heartbeatThreshold = heartbeatThreshold, ?background = background)

    /// <summary>
    ///     Initialize a new local runtime instance with supplied worker count and return a handle.
    /// </summary>
    /// <param name="config">Azure runtime configuration.</param>
    /// <param name="workerCount">Number of local workers to spawn.</param>
    /// <param name="clientId">MBrace.Azure client instance identifier.</param>
    /// <param name="maxWorkItems">Maximum number of concurrent jobs per worker.</param>
    /// <param name="logLevel">Client and local worker logger verbosity level.</param>
    /// <param name="heartbeatInterval">Heartbeat send interval used by worker.</param>
    /// <param name="heartbeatThreshold">Maximum heartbeat threshold after which a worker is to be declared dead.</param>
    /// <param name="background">Run as background instead of windowed process. Defaults to false.</param>
    static member InitOnCurrentMachine(config : Configuration, workerCount : int, ?clientId : string, ?maxWorkItems : int, ?logger : ISystemLogger, 
                                            ?logLevel : LogLevel, ?heartbeatInterval : TimeSpan, ?heartbeatThreshold : TimeSpan, ?background : bool) : AzureCluster =
        let _ = AzureWorker.SpawnMultiple(workerCount, config, ?maxWorkItems = maxWorkItems, ?logLevel = logLevel, ?background = background,
                                                ?heartbeatInterval = heartbeatInterval, ?heartbeatThreshold = heartbeatThreshold)

        AzureCluster.Connect(config, ?clientId = clientId, ?logger = logger, ?logLevel = logLevel)