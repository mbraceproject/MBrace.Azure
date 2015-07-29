namespace MBrace.Azure

open Microsoft.ServiceBus
open Microsoft.WindowsAzure.Storage
open System.Security.Cryptography
open System.Text
open System

/// Configuration identifier.
[<AutoSerializable(true)>]
type ConfigurationId =
    private
      { /// Runtime identifier.
        Id : uint16
        /// Runtime version string
        Version : string
        /// Azure storage connection string hash.
        StorageConnectionStringHash : byte []
        /// Service Bus connection string hash.
        ServiceBusConnectionStringHash : byte []
        /// Service Bus Queue prefix.
        RuntimeQueue : string
        /// Service Bus Topic prefix.
        RuntimeTopic : string
        /// Runtime blob container prefix.
        RuntimeContainer : string
        /// Runtime table prefix.
        RuntimeTable : string
        /// Runtime logs table prefix.
        RuntimeLogsTable : string
        /// User data container prefix.
        UserDataContainer : string
        /// User data table prefix.
        UserDataTable : string }


/// Azure specific Runtime Configuration.
[<AutoSerializable(true)>]
type Configuration =
    { /// Runtime identifier.
      Id : uint16
      /// Runtime version string.
      Version : string
      /// Azure storage connection string.
      StorageConnectionString : string
      /// Service Bus connection string.
      ServiceBusConnectionString : string
      /// Service Bus Queue prefix.
      RuntimeQueue : string
      /// Service Bus Topic prefix.
      RuntimeTopic : string
      /// Runtime blob container prefix.
      RuntimeContainer : string
      /// Runtime table prefix.
      RuntimeTable : string
      /// Runtime logs table prefix.
      RuntimeLogsTable : string
      /// User data container prefix.
      UserDataContainer : string
      /// User data table prefix.
      UserDataTable : string
    }

    /// Returns an Azure Configuration with the default values.
    static member Default =
        { Id                         = 0us
          Version                    = typeof<Configuration>.Assembly.GetName().Version.ToString(4)
          StorageConnectionString    = "your connection string"
          ServiceBusConnectionString = "your connection string"
          // See Azure/ServiceBus name limits
          RuntimeQueue               = "MBraceQueue"
          RuntimeTopic               = "MBraceTopic"
          RuntimeContainer           = "mbraceruntimedata"
          RuntimeTable               = "MBraceRuntimeData"
          RuntimeLogsTable           = "MBraceRuntimeLogs"
          UserDataContainer          = "mbraceuserdata"
          UserDataTable              = "MBraceUserData" }


    /// Append Configuration.Id on all values.
    /// Note : This property should not be used by clients.
    member this.WithAppendedId =
        let version, versionString =
            match Version.TryParse(this.Version) with
            | true, v  -> v, sprintf "%dx%dx%dx%d" v.Major v.Minor v.Build v.Revision
            | false, _ -> failwith <| sprintf "Invalid Version string '%s'" this.Version

        let versionNormalized = version.ToString(4)

        let withVersionAndId s =
            // TODO : Temporary fix to enable GetHandle from newer clients.
            // 0.6.5 clients do not use Version in folder names.
            // < 0.6.1 clients have complete different folder structure.
            if version <= Version(0, 6, 1, 0) then
                raise(NotSupportedException("Connecting to runtimes with Version <= 0.6.1 not supported from newer clients. Use a client with the same version instead."))
            elif version < Version(0, 6, 5, 0) then
                sprintf "%s%05d" s this.Id
            else
                sprintf "%s%s%05d" s versionString this.Id

        let withId s =sprintf "%s%05d" s this.Id

        { this with
            Version = versionNormalized
            RuntimeQueue = withVersionAndId this.RuntimeQueue
            RuntimeTopic = withVersionAndId this.RuntimeTopic
            RuntimeContainer = withVersionAndId this.RuntimeContainer
            RuntimeTable = withVersionAndId this.RuntimeTable
            RuntimeLogsTable = withVersionAndId this.RuntimeLogsTable
            UserDataContainer = withId this.UserDataContainer
            UserDataTable = withId this.UserDataTable
        }

    /// Configuration identifier hash.
    member this.ConfigurationId : ConfigurationId =
        let hashAlgorithm = SHA256Managed.Create()
        let getHash(txt : string) = hashAlgorithm.ComputeHash(Encoding.UTF8.GetBytes txt)

        let store = CloudStorageAccount.Parse(this.StorageConnectionString).Credentials.AccountName
        let sbus = NamespaceManager.CreateFromConnectionString(this.ServiceBusConnectionString).Address.ToString()

        { Id = this.Id
          Version = this.Version
          StorageConnectionStringHash = getHash store
          ServiceBusConnectionStringHash = getHash sbus
          RuntimeQueue = this.RuntimeQueue.ToLower()
          RuntimeTopic = this.RuntimeTopic.ToLower()
          RuntimeContainer = this.RuntimeContainer.ToLower()
          RuntimeTable = this.RuntimeTable.ToLower()
          RuntimeLogsTable = this.RuntimeLogsTable.ToLower()
          UserDataContainer = this.UserDataContainer.ToLower()
          UserDataTable = this.UserDataTable.ToLower()
        }

    /// Return a new copy with altered Version.
    member this.WithVersion(version : string) =
        { this with Version = version }

    /// Return a new copy with altered Id.
    member this.WithId(id) =
        { this with Id = id }


    /// Return a new copy with altered the default user data folders.
    member this.WithUserDataFolders(userDataContainer, userDataTable) =
        { this with
            UserDataContainer = userDataContainer
            UserDataTable = userDataTable }

    /// Return a new copy with altered runtime Queues, Containers and Tables.
    member this.WithRuntimeFolders(runtimeQueue, runtimeTopic, runtimeContainer, runtimeTable, runtimeLogsTable, userDataContainer, userDataTable) =
        { this with
            RuntimeQueue      = runtimeQueue
            RuntimeTopic      = runtimeTopic
            RuntimeContainer  = runtimeContainer
            RuntimeTable      = runtimeTable
            RuntimeLogsTable  = runtimeLogsTable
            UserDataContainer = userDataContainer
            UserDataTable     = userDataTable }

    /// Return a new copy with altered storage connection string.
    member this.WithStorageConnectionString(conn : string) =
        { this with StorageConnectionString = conn }

    /// Return a new copy with altered service bus connection string.
    member this.WithServiceBusConnectionString(conn : string) =
        { this with ServiceBusConnectionString = conn }

namespace MBrace.Azure.Runtime

open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging
open Microsoft.WindowsAzure.Storage
open Nessos.Vagabond
open System
open System.Collections.Concurrent
open System.Reflection
open MBrace.Azure
open System.Net
open System.Threading.Tasks
open MBrace.Runtime.Vagabond
open MBrace.Runtime.Utils
open System.IO
open MBrace.Store.Internals

/// Provides Azure client instances for storage related entities
[<AutoSerializable(false)>]
type StoreClientProvider (config : Configuration) =
    do ServicePointManager.DefaultConnectionLimit <- 512
    do ServicePointManager.Expect100Continue <- false
    do ServicePointManager.UseNagleAlgorithm <- false

    let awaitTask (task : Task) = task.ContinueWith ignore |> Async.AwaitTask

    let acc = lazy CloudStorageAccount.Parse(config.StorageConnectionString)
    member this.TableClient =
        let client = acc.Value.CreateCloudTableClient()
        client.DefaultRequestOptions.RetryPolicy <- RetryPolicies.ExponentialRetry(TimeSpan.FromSeconds(3.), 10)
        client
    member this.BlobClient =
        let client = acc.Value.CreateCloudBlobClient()
        client.DefaultRequestOptions.ParallelOperationThreadCount <- System.Nullable(min 64 (4 * System.Environment.ProcessorCount))
        client.DefaultRequestOptions.SingleBlobUploadThresholdInBytes <- System.Nullable(1L <<< 23) // 8MB, possible ranges: 1..64MB, default 32MB
        client.DefaultRequestOptions.MaximumExecutionTime <- Nullable<_>(TimeSpan.FromMinutes(20.))
        client.DefaultRequestOptions.RetryPolicy <- RetryPolicies.ExponentialRetry(TimeSpan.FromSeconds(3.), 10)
        client
    member this.NamespaceClient = NamespaceManager.CreateFromConnectionString(config.ServiceBusConnectionString)
    member this.QueueClient(queue : string, mode) = QueueClient.CreateFromConnectionString(config.ServiceBusConnectionString, queue, mode)
    member this.SubscriptionClient(topic, name) = SubscriptionClient.CreateFromConnectionString(config.ServiceBusConnectionString, topic, name)
    member this.TopicClient(topic) = TopicClient.CreateFromConnectionString(config.ServiceBusConnectionString, topic)

    member this.ClearUserData() =
        async {
            let! _ = Async.AwaitTask <| this.TableClient.GetTableReference(config.UserDataTable).DeleteIfExistsAsync()
            let! _ = Async.AwaitTask <| this.BlobClient.GetContainerReference(config.UserDataContainer).DeleteIfExistsAsync()
            ()
        }

    member this.ClearRuntimeState() =
        async {
            let! _ = Async.AwaitTask <| this.TableClient.GetTableReference(config.RuntimeTable).DeleteIfExistsAsync()
            let! _ = Async.AwaitTask <| this.BlobClient.GetContainerReference(config.RuntimeContainer).DeleteIfExistsAsync()
            ()
        }

    member this.ClearRuntimeLogs() =
        async {
            let! _ = Async.AwaitTask <| this.TableClient.GetTableReference(config.RuntimeLogsTable).DeleteIfExistsAsync()
            ()
        }

    member this.ClearRuntimeQueues() =
        async {
            do! awaitTask <| this.NamespaceClient.DeleteQueueAsync(config.RuntimeQueue)
            do! awaitTask <| this.NamespaceClient.DeleteTopicAsync(config.RuntimeTopic)
        }

    member this.InitAll() =
        async {
            try
                do! Async.Parallel [|
                        Async.AwaitTask <| this.TableClient.GetTableReference(config.RuntimeTable).CreateIfNotExistsAsync()
                        Async.AwaitTask <| this.TableClient.GetTableReference(config.RuntimeLogsTable).CreateIfNotExistsAsync()
                        Async.AwaitTask <| this.TableClient.GetTableReference(config.UserDataTable).CreateIfNotExistsAsync()
                        Async.AwaitTask <| this.BlobClient.GetContainerReference(config.RuntimeContainer).CreateIfNotExistsAsync()
                        Async.AwaitTask <| this.BlobClient.GetContainerReference(config.UserDataContainer).CreateIfNotExistsAsync()
                    |] |> Async.Ignore
            with e ->
                raise <| InvalidConfigurationException("Invalid Storage Account Configuration", e)
            try
                let! _ = Async.AwaitTask <| this.NamespaceClient.QueueExistsAsync(config.RuntimeQueue)
                ()
            with e ->
                raise <| InvalidConfigurationException("Invalid Service Bus Configuration", e)
        }

[<Sealed;AbstractClass>]
/// Holds configuration specific resources.
type ConfigurationRegistry private () =
    static let registry = new ConcurrentDictionary<ConfigurationId * Type, obj>()

    static member Register<'T>(config : ConfigurationId, item : 'T) : unit =
        registry.TryAdd((config, typeof<'T>), item :> obj)
        |> ignore

    static member Resolve<'T>(config : ConfigurationId) : 'T =
        match registry.TryGetValue((config, typeof<'T>)) with
        | true, v  -> v :?> 'T
        | false, _ -> failwith <| sprintf "Could not resolve Resource of type %A for ConfigurationId %A" config typeof<'T>


type Config private () =
    static let isInitialized = ref false

    static let initVagabond populateDirs (path:string) =
        if populateDirs then ignore <| Directory.CreateDirectory path
        let policy = AssemblyLookupPolicy.ResolveRuntimeStrongNames ||| AssemblyLookupPolicy.ResolveVagabondCache
        Vagabond.Initialize(ignoredAssemblies = [Assembly.GetExecutingAssembly()], cacheDirectory = path, lookupPolicy = policy)

    static let checkInitialized () =
        if not isInitialized.Value then
            invalidOp "Runtime configuration has not been initialized."

    static let initialize (populateDirs : bool) =
        lock isInitialized (fun () ->
            if not isInitialized.Value then
                let _ = System.Threading.ThreadPool.SetMinThreads(256, 256)
                let workingDirectory = WorkingDirectory.CreateWorkingDirectory(cleanup = populateDirs)
                let vagabondDir = Path.Combine(workingDirectory, "vagabond")
                VagabondRegistry.Initialize(fun () -> initVagabond populateDirs vagabondDir)
                isInitialized := true
        )

    /// Default Pickler.
    static member Pickler = checkInitialized() ; VagabondRegistry.Instance.Serializer

    /// Default ISerializer
    static member Serializer = checkInitialized(); new FsPicklerBinaryStoreSerializer() :> ISerializer

    static member Initialize(populateDirs) = initialize populateDirs

    /// Activates the given configuration.
    static member ActivateAsync(config : Configuration, populateDirs) : Async<unit> =
      async {
        initialize populateDirs
        let cp = new StoreClientProvider(config)
        do! cp.InitAll()
        ConfigurationRegistry.Register<StoreClientProvider>(config.ConfigurationId, cp)
    }

    /// Activates the given configuration.
    static member Activate(config) = Async.RunSynchronously(Config.ActivateAsync(config))

    /// Delete Runtime Queue and Topic.
    static member DeleteRuntimeQueues (config : Configuration) =
        async {
            checkInitialized()
            let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config.ConfigurationId)
            do! cp.ClearRuntimeQueues()
        }

    /// Delete Runtime container and table.
    static member DeleteRuntimeState (config : Configuration) =
        async {
            checkInitialized()
            let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config.ConfigurationId)
            do! cp.ClearRuntimeState()
        }

    /// Delete UserData folder.
    static member DeleteUserData (config : Configuration) =
        async {
            checkInitialized()
            let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config.ConfigurationId)
            do! cp.ClearUserData()
        }

    /// Delete RuntimeLogs table.
    static member DeleteRuntimeLogs (config : Configuration) =
        async {
            checkInitialized()
            let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config.ConfigurationId)
            do! cp.ClearRuntimeLogs()
        }
