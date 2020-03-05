namespace MBrace.Azure

#nowarn "444"
#nowarn "445"

open System
open System.Diagnostics
open System.IO
open System.Net

open MBrace.FsPickler

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities
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
/// FsPickler Binary Serializer implementation
type FsPicklerBinarySerializer = MBrace.Runtime.FsPicklerBinarySerializer
/// FsPickler Xml Serializer implementation
type FsPicklerXmlSerializer = MBrace.Runtime.FsPicklerXmlSerializer
/// FsPickler Json Serializer implementation
type FsPicklerJsonSerializer = MBrace.Runtime.FsPicklerJsonSerializer
/// Json.NET serializer implementation
type JsonDotNetSerializer = MBrace.Runtime.JsonDotNetSerializer

/// Azure blob storage utilities
type AzureBlobStorage =
    /// <summary>
    ///     Creates a blob storage client object from given connection string
    /// </summary>
    /// <param name="connectionString">Blob storage connection string</param>
    /// <param name="serializer">Serializer for use with store. Defaults to FsPickler binary serializer.</param>
    static member FromConnectionString(connectionString : string, [<O;D(null:obj)>]?serializer:ISerializer) : CloudFileSystem =
        let blobStore = MBrace.Azure.Store.BlobStore.Create(connectionString)
        let serializer = match serializer with Some s -> s | None -> new FsPicklerBinarySerializer() :> _
        new CloudFileSystem(blobStore, serializer)

    /// <summary>
    ///      Creates a blob storage client object from given account credentials
    /// </summary>
    /// <param name="accountName">Storage account name.</param>
    /// <param name="accountKey">Storage account key.</param>
    /// <param name="serializer">Serializer for use with store. Defaults to FsPickler binary serializer.</param>
    static member FromCredentials(accountName : string, accountKey : string, [<O;D(null:obj)>]?serializer:ISerializer) : CloudFileSystem =
        let account = AzureStorageAccount.FromCredentials(accountName, accountKey)
        AzureBlobStorage.FromConnectionString(account.ConnectionString, ?serializer = serializer)

/// Local Azure Standalone worker management methods
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
    /// <param name="quiet">Suppress output to stdout by worker instance. Defaults to false.</param>
    /// <param name="detach">Worker node will offload mbrace code execution in a detached child process. Defaults to false.</param>
    /// <param name="logLevel">Client and local worker logger verbosity level.</param>
    /// <param name="logFile">Specify local path to system logfile for worker process.</param>
    /// <param name="heartbeatInterval">Heartbeat send interval used by worker.</param>
    /// <param name="heartbeatThreshold">Maximum heartbeat threshold after which a worker is to be declared dead.</param>
    /// <param name="background">Run as background instead of windowed process. Defaults to false.</param>
    static member Spawn([<O;D(null:obj)>]?config : Configuration, [<O;D(null:obj)>]?workerId : string, [<O;D(null:obj)>]?workingDirectory : string, [<O;D(null:obj)>]?maxWorkItems : int, [<O;D(null:obj)>]?logLevel : LogLevel, 
                            [<O;D(null:obj)>]?quiet : bool, [<O;D(null:obj)>]?detach : bool, [<O;D(null:obj)>]?logFile : string, [<O;D(null:obj)>]?heartbeatInterval : TimeSpan, [<O;D(null:obj)>]?heartbeatThreshold : TimeSpan, [<O;D(null:obj)>]?background : bool) : Process =
        let background = defaultArg background false
        let quiet = defaultArg quiet ProcessConfiguration.IsUnix
        let exe = AzureWorker.LocalExecutable
        let cli = ArgumentConfiguration.Create(?config = config, ?workingDirectory = workingDirectory, ?maxWorkItems = maxWorkItems, ?logLevel = logLevel, quiet = quiet, ?detach = detach,
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
    /// <param name="quiet">Suppress output to stdout by worker instance. Defaults to false.</param>
    /// <param name="detach">Worker node will offload mbrace code execution in a detached child process. Defaults to false.</param>
    /// <param name="logLevel">Client and local worker logger verbosity level.</param>
    /// <param name="heartbeatInterval">Heartbeat send interval used by worker.</param>
    /// <param name="heartbeatThreshold">Maximum heartbeat threshold after which a worker is to be declared dead.</param>
    /// <param name="background">Run as background instead of windowed process. Defaults to false.</param>
    static member SpawnMultiple(workerCount : int, [<O;D(null:obj)>]?config : Configuration, [<O;D(null:obj)>]?maxWorkItems : int, [<O;D(null:obj)>]?logLevel : LogLevel,
                                    [<O;D(null:obj)>]?quiet : bool, [<O;D(null:obj)>]?detach : bool, [<O;D(null:obj)>]?heartbeatInterval : TimeSpan, [<O;D(null:obj)>]?heartbeatThreshold : TimeSpan, [<O;D(null:obj)>]?background : bool) : Process [] =

        let _ = AzureWorker.LocalExecutable // force early exception
        if workerCount < 1 then invalidArg "workerCount" "must be positive."
        let spawn _ = AzureWorker.Spawn(?config = config, ?maxWorkItems = maxWorkItems, ?logLevel = logLevel, ?quiet = quiet, ?detach = detach,
                                            ?heartbeatInterval = heartbeatInterval, ?heartbeatThreshold = heartbeatThreshold, ?background = background)

        [|1 .. workerCount|] |> Array.Parallel.map spawn

/// <summary>
///     Windows Azure Cluster management client. Provides methods for management, execution and debugging of MBrace processes in Azure.
/// </summary>
[<AutoSerializable(false); NoEquality; NoComparison>]
type AzureCluster private (manager : ClusterManager, faultPolicy : FaultPolicy option) =
    inherit MBraceClient(manager, match faultPolicy with None -> FaultPolicy.NoRetry | Some fp -> fp)
    static do ProcessConfiguration.InitAsClient()
    let hashId = manager.ClusterId.Hash

    /// Gets the Azure storage account name used by the cluster
    member this.StorageAccount = manager.Configuration.StorageAccount
    /// Gets the Azure service bus account name used by the cluster
    member this.ServiceBusAccount = manager.Configuration.ServiceBusAccount
    /// Cluster configuration hash identifier
    member this.Hash = hashId

    /// Gets a copy of the configuration object used for the runtime
    [<DebuggerBrowsable(DebuggerBrowsableState.Never)>]
    member this.Configuration = FsPickler.Clone manager.Configuration

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
    ///     Delete and re-activate runtime state.
    ///     Using 'Reset' may cause unexpected behavior in clients and workers.
    ///     Workers should be restarted manually.
    /// </summary>
    /// <param name="deleteQueues">Delete Configuration queue and topic. Defaults to true.</param>
    /// <param name="deleteRuntimeState">Delete Configuration table and containers. Defaults to true.</param>
    /// <param name="deleteLogs">Delete Configuration logs table. Defaults to true.</param>
    /// <param name="deleteUserData">Delete Configuration UserData table and container. Defaults to false.</param>
    /// <param name="deleteAssemblyData">Delete assembly data container from blob store. Defaults to false.</param>
    /// <param name="force">Ignore active workers. Defaults to false.</param>
    /// <param name="reactivate">Reactivate configuration. Defaults to true.</param>
    [<CompilerMessage("Using 'Reset' may cause unexpected behavior in clients and workers.", 445)>]
    member this.ResetAsync([<O;D(null:obj)>]?deleteQueues : bool, [<O;D(null:obj)>]?deleteRuntimeState : bool, [<O;D(null:obj)>]?deleteLogs : bool, [<O;D(null:obj)>]?deleteAssemblyData : bool, [<O;D(null:obj)>]?force : bool, [<O;D(null:obj)>]?reactivate : bool) : Async<unit> =

        manager.ResetCluster(?deleteQueues = deleteQueues, ?deleteRuntimeState = deleteRuntimeState, ?deleteLogs = deleteLogs, 
                                ?deleteAssemblyData = deleteAssemblyData, 
                                ?force = force, ?reactivate = reactivate)

    /// <summary>
    ///     Delete and re-activate runtime state.
    ///     Using 'Reset' may cause unexpected behavior in clients and workers.
    ///     Workers should be restarted manually.
    /// </summary>
    /// <param name="deleteQueues">Delete Configuration queue and topic. Defaults to true.</param>
    /// <param name="deleteRuntimeState">Delete Configuration table and containers. Defaults to true.</param>
    /// <param name="deleteLogs">Delete Configuration logs table. Defaults to true.</param>
    /// <param name="deleteUserData">Delete Configuration UserData table and container. Defaults to false.</param>
    /// <param name="deleteAssemblyData">Delete assembly data container from blob store. Defaults to false.</param>
    /// <param name="force">Ignore active workers. Defaults to false.</param>
    /// <param name="reactivate">Reactivate configuration. Defaults to true.</param>
    [<CompilerMessage("Using 'Reset' may cause unexpected behavior in clients and workers.", 445)>]
    member this.Reset([<O;D(null:obj)>]?deleteQueues : bool, [<O;D(null:obj)>]?deleteRuntimeState : bool, [<O;D(null:obj)>]?deleteLogs : bool,
                        [<O;D(null:obj)>]?deleteAssemblyData : bool, [<O;D(null:obj)>]?force : bool, [<O;D(null:obj)>]?reactivate : bool) : unit =

        this.ResetAsync(?deleteQueues = deleteQueues, ?deleteRuntimeState = deleteRuntimeState, ?deleteLogs = deleteLogs, 
                                ?deleteAssemblyData = deleteAssemblyData, 
                                ?force = force, ?reactivate = reactivate)
        |> Async.RunSync

    /// <summary>
    ///     Culls cluster workers that have stopped sending heartbeats
    ///     for a duration larger than the supplied threshold parameter.
    /// </summary>
    /// <param name="heartbeatThreshold">Stopped heartbeat cull threshold.</param>
    member this.CullNonResponsiveWorkersAsync(heartbeatThreshold : TimeSpan) : Async<unit> =
        manager.WorkerManager.CullNonResponsiveWorkers(heartbeatThreshold)

    /// <summary>
    ///     Culls cluster workers that have stopped sending heartbeats
    ///     for a duration larger than the supplied threshold parameter.
    /// </summary>
    /// <param name="heartbeatThreshold">Stopped heartbeat cull threshold.</param>
    member this.CullNonResponsiveWorkers(heartbeatThreshold : TimeSpan) : unit =
        this.CullNonResponsiveWorkersAsync(heartbeatThreshold) |> Async.RunSync

    /// <summary>
    ///     Spawns a worker instance in the local machine, subscribed to the current cluster configuration.
    /// </summary>
    /// <param name="workerId">Unique worker identifier in the cluster.</param>
    /// <param name="workingDirectory">Local working directory for the worker process.</param>
    /// <param name="maxWorkItems">Maximum number of concurrent jobs per worker.</param>
    /// <param name="quiet">Suppress output to stdout by worker instance. Defaults to false.</param>
    /// <param name="logFile">Specify local path to system logfile for worker process.</param>
    /// <param name="logLevel">Client and local worker logger verbosity level.</param>
    /// <param name="heartbeatInterval">Heartbeat send interval used by worker.</param>
    /// <param name="detach">Worker node will offload mbrace code execution in a detached child process. Defaults to false.</param>
    /// <param name="heartbeatThreshold">Maximum heartbeat threshold after which a worker is to be declared dead.</param>
    /// <param name="background">Run as background instead of windowed process. Defaults to false.</param>
    member this.AttachLocalWorker([<O;D(null:obj)>]?workerId : string, [<O;D(null:obj)>]?workingDirectory : string, [<O;D(null:obj)>]?maxWorkItems : int, [<O;D(null:obj)>]?logFile : string, [<O;D(null:obj)>]?logLevel:LogLevel,
                                    [<O;D(null:obj)>]?quiet : bool, [<O;D(null:obj)>]?detach : bool, [<O;D(null:obj)>]?heartbeatInterval : TimeSpan, [<O;D(null:obj)>]?heartbeatThreshold : TimeSpan, [<O;D(null:obj)>]?background : bool) : unit =
        ignore <| AzureWorker.Spawn(manager.Configuration, ?workerId = workerId, ?workingDirectory = workingDirectory, ?maxWorkItems = maxWorkItems, ?logFile = logFile, 
                            ?quiet = quiet, ?detach = detach, ?logLevel = logLevel, ?heartbeatInterval = heartbeatInterval, ?heartbeatThreshold = heartbeatThreshold, ?background = background)

    /// <summary>
    ///     Spawns worker instances in the local machine, subscribed to the current cluster configuration.
    /// </summary>
    /// <param name="workerCount">Number of local workers to spawn.</param>
    /// <param name="maxWorkItems">Maximum number of concurrent jobs per worker.</param>
    /// <param name="quiet">Suppress output to stdout by worker instance. Defaults to false.</param>
    /// <param name="logFile">Specify local path to system logfile for worker process.</param>
    /// <param name="logLevel">Client and local worker logger verbosity level.</param>
    /// <param name="heartbeatInterval">Heartbeat send interval used by worker.</param>
    /// <param name="heartbeatThreshold">Maximum heartbeat threshold after which a worker is to be declared dead.</param>
    /// <param name="background">Run as background instead of windowed process. Defaults to false.</param>
    member this.AttachLocalWorkers(workerCount : int, [<O;D(null:obj)>]?maxWorkItems : int, [<O;D(null:obj)>]?logLevel : LogLevel, 
                                    [<O;D(null:obj)>]?quiet : bool, [<O;D(null:obj)>]?heartbeatInterval : TimeSpan, [<O;D(null:obj)>]?heartbeatThreshold : TimeSpan, [<O;D(null:obj)>]?background : bool) : unit =

        ignore <| AzureWorker.SpawnMultiple(workerCount, config = manager.Configuration, ?maxWorkItems = maxWorkItems, ?logLevel = logLevel, ?background = background,
                                                ?quiet = quiet, ?heartbeatInterval = heartbeatInterval, ?heartbeatThreshold = heartbeatThreshold)


    /// <summary>
    ///     Connects to an MBrace-on-Azure cluster as identified by provided configuration object.
    ///     If successful returns a management handle object to the cluster.
    /// </summary>
    /// <param name="config">Runtime configuration.</param>
    /// <param name="clientId">MBrace.Azure client instance identifier.</param>
    /// <param name="clientId">Custom client id for this instance.</param>
    /// <param name="faultPolicy">The default fault policy to be used by the cluster. Defaults to NoRetry.</param>
    /// <param name="logger">Custom logger to attach in client.</param>
    /// <param name="logLevel">Logger verbosity level. Defaults to LogLevel.Info.</param>
    static member ConnectAsync (config : Configuration, [<O;D(null:obj)>]?clientId : string, [<O;D(null:obj)>]?faultPolicy : FaultPolicy, [<O;D(null:obj)>]?logger : ISystemLogger, [<O;D(null:obj)>]?logLevel : LogLevel) : Async<AzureCluster> = async {
        let logLevel = defaultArg logLevel LogLevel.Info
        let hostProc = Diagnostics.Process.GetCurrentProcess()
        let clientId = defaultArg clientId <| sprintf "%s-%s-%05d" (System.Net.Dns.GetHostName()) hostProc.ProcessName hostProc.Id
        let! manager = ClusterManager.Create(config, ?systemLogger = logger)
        let! storageLogger = manager.SystemLoggerManager.CreateLogWriter(clientId)
        let _ = manager.LocalLoggerManager.AttachLogger(storageLogger)
        manager.LocalLoggerManager.LogLevel <- logLevel
        return new AzureCluster(manager, faultPolicy)
    }

    /// <summary>
    ///     Connects to an MBrace-on-Azure cluster as identified by provided configuration object.
    ///     If successful returns a management handle object to the cluster.
    /// </summary>
    /// <param name="config">Runtime configuration.</param>
    /// <param name="clientId">MBrace.Azure client instance identifier.</param>
    /// <param name="faultPolicy">The default fault policy to be used by the cluster. Defaults to NoRetry.</param>
    /// <param name="logger">Custom logger to attach in client.</param>
    /// <param name="logLevel">Logger verbosity level.</param>
    static member Connect (config : Configuration, [<O;D(null:obj)>]?clientId : string, [<O;D(null:obj)>]?faultPolicy : FaultPolicy, [<O;D(null:obj)>]?logger : ISystemLogger, [<O;D(null:obj)>]?logLevel : LogLevel) : AzureCluster =
        AzureCluster.ConnectAsync(config, ?clientId = clientId, ?faultPolicy = faultPolicy, ?logger = logger, ?logLevel = logLevel)
        |> Async.RunSync

    /// <summary>
    ///     Connects to an MBrace-on-Azure cluster as identified by provided store and service bus connection strings.
    ///     If successful returns a management handle object to the cluster.
    /// </summary>
    /// <param name="storageConnectionString">Azure Storage connection string.</param>
    /// <param name="serviceBusConnectionString">Azure Service Bus connection string.</param>
    /// <param name="clientId">MBrace.Azure client instance identifier.</param>
    /// <param name="faultPolicy">The default fault policy to be used by the cluster. Defaults to NoRetry.</param>
    /// <param name="logger">Custom logger to attach in client.</param>
    /// <param name="logLevel">Logger verbosity level.</param>
    static member Connect(storageConnectionString : string, serviceBusConnectionString : string, [<O;D(null:obj)>]?clientId : string, [<O;D(null:obj)>]?faultPolicy : FaultPolicy, [<O;D(null:obj)>]?logger : ISystemLogger, [<O;D(null:obj)>]?logLevel : LogLevel) : AzureCluster = 
        AzureCluster.Connect(new Configuration(storageConnectionString, serviceBusConnectionString), ?clientId = clientId, ?faultPolicy = faultPolicy, ?logger = logger, ?logLevel = logLevel)

    /// <summary>
    ///     Initialize a new local runtime instance with supplied worker count and return a handle.
    /// </summary>
    /// <param name="config">Azure runtime configuration.</param>
    /// <param name="workerCount">Number of local workers to spawn.</param>
    /// <param name="clientId">MBrace.Azure client instance identifier.</param>
    /// <param name="faultPolicy">The default fault policy to be used by the cluster. Defaults to NoRetry.</param>
    /// <param name="maxWorkItems">Maximum number of concurrent jobs per worker.</param>
    /// <param name="quiet">Suppress output to stdout by worker instance. Defaults to false.</param>
    /// <param name="logLevel">Client and local worker logger verbosity level.</param>
    /// <param name="heartbeatInterval">Heartbeat send interval used by worker.</param>
    /// <param name="heartbeatThreshold">Maximum heartbeat threshold after which a worker is to be declared dead.</param>
    /// <param name="background">Run as background instead of windowed process. Defaults to false.</param>
    static member InitOnCurrentMachine(config : Configuration, workerCount : int, [<O;D(null:obj)>]?clientId : string, [<O;D(null:obj)>]?faultPolicy : FaultPolicy, [<O;D(null:obj)>]?maxWorkItems : int, [<O;D(null:obj)>]?logger : ISystemLogger, 
                                            [<O;D(null:obj)>]?quiet : bool, [<O;D(null:obj)>]?logLevel : LogLevel, [<O;D(null:obj)>]?heartbeatInterval : TimeSpan, [<O;D(null:obj)>]?heartbeatThreshold : TimeSpan, [<O;D(null:obj)>]?background : bool) : AzureCluster =
        let _ = AzureWorker.SpawnMultiple(workerCount, config, ?maxWorkItems = maxWorkItems, ?logLevel = logLevel, ?background = background,
                                                ?quiet = quiet, ?heartbeatInterval = heartbeatInterval, ?heartbeatThreshold = heartbeatThreshold)

        AzureCluster.Connect(config, ?clientId = clientId, ?faultPolicy = faultPolicy, ?logger = logger, ?logLevel = logLevel)