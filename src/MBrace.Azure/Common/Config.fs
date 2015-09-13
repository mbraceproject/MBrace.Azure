namespace MBrace.Azure.Runtime

open System

/// Configuration identifier.
/// Holds hashes of connection strings and normalized runtime folders.
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
        /// Service Bus Queue.
        RuntimeQueue : string
        /// Service Bus Topic.
        RuntimeTopic : string
        /// Runtime blob container.
        RuntimeContainer : string
        /// Runtime table.
        RuntimeTable : string
        /// Runtime logs table.
        RuntimeLogsTable : string
        /// User data container.
        UserDataContainer : string
        /// Vagabond Assembly Container.
        VagabondContainer : string
        /// CloudValue Persist Container.
        CloudValueContainer : string
        /// User data table.
        UserDataTable : string }
    with
        interface MBrace.Runtime.IRuntimeId with
            member this.Id: string = 
                let toBytes (text : string) = System.Text.Encoding.UTF8.GetBytes(text)
                [
                    this.Id |> sprintf "%05d" |> toBytes
                    this.Version              |> toBytes
                    this.StorageConnectionStringHash
                    this.ServiceBusConnectionStringHash
                    this.RuntimeQueue      + ";" |> toBytes
                    this.RuntimeTopic      + ";" |> toBytes
                    this.RuntimeContainer  + ";" |> toBytes
                    this.RuntimeTable      + ";" |> toBytes
                    this.RuntimeLogsTable  + ";" |> toBytes
                    this.UserDataContainer + ";" |> toBytes
                    this.UserDataTable     + ";" |> toBytes
                ] 
                |> Array.concat
                |> Convert.ToBase64String
            

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

    /// Azure Storage container used for Vagabond assembly dependencies.
    member val AssemblyContainer          = "vagabond" with get, set

    /// Azure Storage container used for CloudValue persistence.
    member val CloudValueContainer        = "cloudvalue" with get, set

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

        let _ = Validate.storageConn this.StorageConnectionString
        let _ = Validate.serviceBusConn this.ServiceBusConnectionString

        let store = CloudStorageAccount.Parse(this.StorageConnectionString).Credentials.AccountName
        let sbus = NamespaceManager.CreateFromConnectionString(this.ServiceBusConnectionString).Address.ToString()

        {
            Id                             = this.Id
            Version                        = versionNormalized
            StorageConnectionStringHash    = getHash store
            ServiceBusConnectionStringHash = getHash sbus
            RuntimeQueue                   = (appendVersionAndId this.RuntimeQueue       ).ToLower() |> Validate.queueName
            RuntimeTopic                   = (appendVersionAndId this.RuntimeTopic       ).ToLower() |> Validate.queueName
            RuntimeContainer               = (appendVersionAndId this.RuntimeContainer   ).ToLower() |> Validate.containerName
            RuntimeTable                   = (appendVersionAndId this.RuntimeTable       ).ToLower() |> Validate.tableName
            RuntimeLogsTable               = (appendVersionAndId this.RuntimeLogsTable   ).ToLower() |> Validate.tableName
            VagabondContainer              = (appendVersionAndId this.AssemblyContainer  ).ToLower() |> Validate.containerName
            CloudValueContainer            = (appendVersionAndId this.CloudValueContainer).ToLower() |> Validate.containerName
            UserDataContainer              = (appendId this.UserDataContainer            ).ToLower() |> Validate.containerName
            UserDataTable                  = (appendId this.UserDataTable                ).ToLower() |> Validate.tableName
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
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Store
open System.IO

/// Provides Azure client instances for storage related entities
[<AutoSerializable(false)>]
type StoreClientProvider (config : Configuration) =
    let config, store, sbus = config.GetConfigurationId(), config.StorageConnectionString, config.ServiceBusConnectionString
    do ServicePointManager.DefaultConnectionLimit <- 512
    do ServicePointManager.Expect100Continue <- false
    do ServicePointManager.UseNagleAlgorithm <- false

    let awaitTask (task : Task) = task.ContinueWith ignore |> Async.AwaitTask

    let acc = lazy CloudStorageAccount.Parse(store)
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
    member this.NamespaceClient = NamespaceManager.CreateFromConnectionString(sbus)
    member this.QueueClient(queue : string, mode) = QueueClient.CreateFromConnectionString(sbus, queue, mode)
    member this.SubscriptionClient(topic, name) = SubscriptionClient.CreateFromConnectionString(sbus, topic, name)
    member this.TopicClient(topic) = TopicClient.CreateFromConnectionString(sbus, topic)

    member this.ClearUserData() =
        async {
            let! _ = Async.AwaitTask <| this.TableClient.GetTableReference(config.UserDataTable).DeleteIfExistsAsync()
            let! _ = Async.AwaitTask <| this.BlobClient.GetContainerReference(config.UserDataContainer).DeleteIfExistsAsync()
            do! this.BlobClient.GetContainerReference(config.CloudValueContainer).DeleteIfExistsAsync()
            ()
        }

    member this.ClearVagabondData() =
        async {
            do! this.BlobClient.GetContainerReference(config.VagabondContainer).DeleteIfExistsAsync()
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
                        Async.AwaitTask <| this.BlobClient.GetContainerReference(config.CloudValueContainer).CreateIfNotExistsAsync()
                        Async.AwaitTask <| this.BlobClient.GetContainerReference(config.VagabondContainer).CreateIfNotExistsAsync()
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
    static let mutable objectCache = Unchecked.defaultof<InMemoryCache>
    static let mutable localFileStore = Unchecked.defaultof<FileSystemStore>
    static let mutable workingDirectory = Unchecked.defaultof<string>

    static let checkInitialized () =
        if not isInitialized.Value then
            invalidOp "Runtime configuration has not been initialized."

    static let initGlobalState path populateDirs isClientInstance =
        lock isInitialized (fun () ->
            if not isInitialized.Value then
                let _ = System.Threading.ThreadPool.SetMinThreads(256, 256)
                workingDirectory <- WorkingDirectory.CreateWorkingDirectory(?path = path, cleanup = populateDirs)
                let vagabondDir = Path.Combine(workingDirectory, "vagabond")
                if populateDirs then ignore <| WorkingDirectory.CreateWorkingDirectory(vagabondDir, cleanup = false)
                VagabondRegistry.Initialize(vagabondDir, isClientSession = isClientInstance)
                objectCache <- InMemoryCache.Create(name = "MBrace.Azure object cache")
                localFileStore <- FileSystemStore.Create(rootPath = Path.Combine(workingDirectory, "localStore"), create = populateDirs)
                isInitialized := true
        )

    /// Default FsPicklerSerializer instance.
    static member Pickler = checkInitialized() ; VagabondRegistry.Instance.Serializer

    /// Default ISerializer
    static member Serializer = checkInitialized(); new VagabondFsPicklerBinarySerializer() :> ISerializer

    /// Working Directory used by current global state.
    static member WorkingDirectory = checkInitialized(); workingDirectory

    /// In-Memory cache
    static member ObjectCache = checkInitialized(); objectCache

    /// Local file system store
    static member FileStore = checkInitialized(); localFileStore

    /// Initializes MBrace.Azure global state for client process.
    static member InitClientGlobalState() = initGlobalState None true true
    /// Initializes MBrace.Azure global state for worker process, master AppDomain.
    static member InitWorkerGlobalState() = initGlobalState None true false
    /// Initializes MBrace.Azure global state for worker AppDomain.
    static member InitAppDomainGlobalState(workingDirectory) = initGlobalState (Some workingDirectory) false false
        

    static member ReactivateAsync(config : ConfigurationId) =
        async {
            checkInitialized()
            let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config)
            do! cp.InitAll()
        }

    /// Activates the given configuration.
    static member ActivateAsync(config : Configuration) : Async<unit> =
      async {
        checkInitialized()
        let cp = new StoreClientProvider(config)
        do! cp.InitAll()
        ConfigurationRegistry.Register<StoreClientProvider>(config.GetConfigurationId(), cp)
    }

    /// Activates the given configuration.
    static member Activate(config : Configuration) =
        Async.RunSync(Config.ActivateAsync(config))

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

    static member DeleteVagabondData (config : ConfigurationId) =
        async {
            checkInitialized()
            let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config)
            do! cp.ClearVagabondData()
        }

    /// Delete RuntimeLogs table.
    static member DeleteRuntimeLogs (config : ConfigurationId) =
        async {
            checkInitialized()
            let cp = ConfigurationRegistry.Resolve<StoreClientProvider>(config)
            do! cp.ClearRuntimeLogs()
        }
