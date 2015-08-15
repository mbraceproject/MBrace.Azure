namespace MBrace.Azure.Runtime

/// Configuration identifier.
[<AutoSerializable(true)>]
type ConfigurationId =
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

namespace MBrace.Azure

open Microsoft.ServiceBus
open Microsoft.WindowsAzure.Storage
open System.Security.Cryptography
open System.Text
open System
open MBrace.Azure.Runtime
open MBrace.Runtime.Utils.String

// Make this serializable for client/local runtime communication.
[<AutoSerializable(true)>]
/// MBrace.Azure runtime configuration.
type Configuration(storageConnectionString : string, serviceBusConnectionString : string) = 

    /// Runtime identifier, used for runtime isolation when using the same storage/servicebus accounts. Defaults to 0.
    member val Id                         = 0us with get, set

    /// Runtime version this configuration is targeting. Default to current assembly version.
    member val Version                    = typeof<Configuration>.Assembly.GetName().Version.ToString(4) with get, set

    /// Azure Storage connection string.
    member val StorageConnectionString    = storageConnectionString with get, set

    /// Azure Service Bus connection string.
    member val ServiceBusConnectionString = serviceBusConnectionString with get, set

    /// Service Bus queue used by the runtime.
    member val RuntimeQueue               = "MBraceQueue" with get, set

    /// Service Bus topic used by the runtime.
    member val RuntimeTopic               = "MBraceTopic" with get, set

    /// Azure Storage container used by the runtime.
    member val RuntimeContainer           = "mbraceruntimedata" with get, set

    /// Azure Storage table used by the runtime.
    member val RuntimeTable               = "MBraceRuntimeData" with get, set

    /// Azure Storage table used by the runtime for storing logs.
    member val RuntimeLogsTable           = "MBraceRuntimeLogs" with get, set

    /// Azure Storage container used for user data.
    member val UserDataContainer          = "mbraceuserdata" with get, set

    /// Azure Storage table used for user data.
    member val UserDataTable              = "MBraceUserData" with get, set
    
    /// Append version to given configuration e.g. $RuntimeQueue$Version. Defaults to true.
    member val UseVersionPostfix          = true with get, set

    /// Append runtime id to given configuration e.g. $RuntimeQueue$Version$Id. Defaults to true.
    member val UseIdPostfix               = true with get, set

    /// Validate configuration and construct ConfigurationId.
    member this.GetConfigurationId () : ConfigurationId = 
        let versionNormalized, versionPostfix =
            match Version.TryParse(this.Version) with
            | true, v  -> v.ToString(4), sprintf "%dx%dx" v.Major v.Minor
            | false, _ -> raise <| InvalidConfigurationException(sprintf "Invalid Version string '%s'" this.Version)
        
        let appendId text =
            if this.UseIdPostfix then sprintf "%s%05d" text this.Id else text
        let appendVersionAndId (text : string) =
            let sb = new StringBuilder()
            (stringB {
                yield text
                if this.UseVersionPostfix then yield versionPostfix
                if this.UseIdPostfix then yield sprintf "%05d" this.Id
            }) sb
            sb.ToString()

        let hashAlgorithm = SHA256Managed.Create()
        let getHash(txt : string) = hashAlgorithm.ComputeHash(Encoding.UTF8.GetBytes txt)

        let store = CloudStorageAccount.Parse(this.StorageConnectionString).Credentials.AccountName
        let sbus = NamespaceManager.CreateFromConnectionString(this.ServiceBusConnectionString).Address.ToString()

        {
            Id                             = this.Id
            Version                        = versionNormalized
            StorageConnectionStringHash    = getHash store
            ServiceBusConnectionStringHash = getHash sbus
            RuntimeQueue                   = (appendVersionAndId this.RuntimeQueue    ).ToLower()
            RuntimeTopic                   = (appendVersionAndId this.RuntimeTopic    ).ToLower()
            RuntimeContainer               = (appendVersionAndId this.RuntimeContainer).ToLower()
            RuntimeTable                   = (appendVersionAndId this.RuntimeTable    ).ToLower()
            RuntimeLogsTable               = (appendVersionAndId this.RuntimeLogsTable).ToLower()
            UserDataContainer              = (appendId this.UserDataContainer         ).ToLower()
            UserDataTable                  = (appendId this.UserDataTable             ).ToLower()
        }

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

    static member ReactivateAsync(config : ConfigurationId) =
        async {
            checkInitialized()
            let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config)
            do! cp.InitAll()        
        }

    /// Activates the given configuration.
    static member ActivateAsync(config : Configuration, populateDirs) : Async<unit> =
      async {
        initialize populateDirs
        let cp = new StoreClientProvider(config)
        do! cp.InitAll()
        ConfigurationRegistry.Register<StoreClientProvider>(config.GetConfigurationId(), cp)
    }

    /// Activates the given configuration.
    static member Activate(config) = Async.RunSynchronously(Config.ActivateAsync(config))

    /// Delete Runtime Queue and Topic.
    static member DeleteRuntimeQueues (config : ConfigurationId) =
        async {
            checkInitialized()
            let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config)
            do! cp.ClearRuntimeQueues()
        }

    /// Delete Runtime container and table.
    static member DeleteRuntimeState (config : ConfigurationId) =
        async {
            checkInitialized()
            let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config)
            do! cp.ClearRuntimeState()
        }

    /// Delete UserData folder.
    static member DeleteUserData (config : ConfigurationId) =
        async {
            checkInitialized()
            let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config)
            do! cp.ClearUserData()
        }

    /// Delete RuntimeLogs table.
    static member DeleteRuntimeLogs (config : ConfigurationId) =
        async {
            checkInitialized()
            let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config)
            do! cp.ClearRuntimeLogs()
        }
