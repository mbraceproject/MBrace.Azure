namespace MBrace.Azure

open Microsoft.ServiceBus
open Microsoft.WindowsAzure.Storage
open System.Security.Cryptography
open System.Text

/// Configuration identifier.
type ConfigurationId = 
    private 
      { /// Runtime identifier.
        Id : uint32
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
[<AutoSerializable(false)>]
type Configuration = 
    { /// Runtime identifier.
      Id : uint32
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
        { Id                         = 0u
          StorageConnectionString    = "your connection string"
          ServiceBusConnectionString = "your connection string"
          RuntimeQueue               = "mbraceruntimequeue"
          RuntimeTopic               = "mbraceruntimetopic"
          RuntimeContainer           = "mbraceruntimedata"
          RuntimeTable               = "mbraceruntimedata"
          RuntimeLogsTable           = "mbraceruntimelogs"
          UserDataContainer          = "mbraceuserdata"
          UserDataTable              = "mbraceuserdata" }


    /// Append Configuration.Id on all values.
    member this.WithAppendedId =
        let withId s = sprintf "%s%10d" s this.Id
        { this with
            RuntimeQueue = withId this.RuntimeQueue
            RuntimeTopic = withId this.RuntimeTopic
            RuntimeContainer = withId this.RuntimeContainer
            RuntimeTable = withId this.RuntimeTable
            RuntimeLogsTable = withId this.RuntimeLogsTable
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
            
        
    /// Returns a new copy with altered storage connection string.
    member this.WithStorageConnectionString(conn : string) =
        { this with StorageConnectionString = conn }

    /// Returns a new copy with altered service bus connection string.
    member this.WithServiceBusConnectionString(conn : string) =
        { this with ServiceBusConnectionString = conn }

namespace MBrace.Azure.Runtime

open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging
open Microsoft.WindowsAzure.Storage
open Nessos.FsPickler
open MBrace.Runtime
open Nessos.Vagabond
open System
open System.Collections.Concurrent
open System.Reflection
open System.Security.Cryptography
open System.Text
open System.Threading
open MBrace.Azure

[<AutoSerializable(false)>]
type ClientProvider (config : Configuration) =
    let acc = CloudStorageAccount.Parse(config.StorageConnectionString)
    do System.Net.ServicePointManager.Expect100Continue <- false
    do System.Net.ServicePointManager.UseNagleAlgorithm <- false

    member __.TableClient = acc.CreateCloudTableClient()
    member __.BlobClient = 
        let client = acc.CreateCloudBlobClient()
        client.DefaultRequestOptions.ParallelOperationThreadCount <- System.Nullable(4 * System.Environment.ProcessorCount)
        client.DefaultRequestOptions.SingleBlobUploadThresholdInBytes <- System.Nullable(1L <<< 23) // 8MB, possible ranges: 1..64MB, default 32MB
        client.DefaultRequestOptions.MaximumExecutionTime <- Nullable<_>(TimeSpan.FromMinutes(10.))
        client
    member __.NamespaceClient = NamespaceManager.CreateFromConnectionString(config.ServiceBusConnectionString)
    member __.QueueClient(queue : string, mode) = QueueClient.CreateFromConnectionString(config.ServiceBusConnectionString, queue, mode)
    member __.SubscriptionClient(topic, name) = SubscriptionClient.CreateFromConnectionString(config.ServiceBusConnectionString, topic, name)
    member __.TopicClient(topic) = TopicClient.CreateFromConnectionString(config.ServiceBusConnectionString, topic)

    member __.ClearUserData() =
        async {
            let! _ = Async.AwaitTask <| __.TableClient.GetTableReference(config.UserDataTable).DeleteIfExistsAsync()
            let! _ = Async.AwaitTask <| __.BlobClient.GetContainerReference(config.UserDataContainer).DeleteIfExistsAsync()
            ()
        }

    member __.ClearRuntimeState() =
        async { 
            let! _ = Async.AwaitTask <| __.TableClient.GetTableReference(config.RuntimeTable).DeleteIfExistsAsync()
            let! _ = Async.AwaitTask <| __.TableClient.GetTableReference(config.RuntimeLogsTable).DeleteIfExistsAsync()
            let! _ = Async.AwaitTask <| __.BlobClient.GetContainerReference(config.RuntimeContainer).DeleteIfExistsAsync()
            do! __.ClearRuntimeQueues()
        }

    member __.ClearRuntimeQueues() =
        async {
            let! _ = Async.AwaitIAsyncResult <| __.NamespaceClient.DeleteQueueAsync(config.RuntimeQueue)
            let! _ = Async.AwaitIAsyncResult <| __.NamespaceClient.DeleteTopicAsync(config.RuntimeTopic)
            ()
        }

    member __.InitAll() =
        async {
            let! _ = Async.AwaitTask <| __.TableClient.GetTableReference(config.RuntimeTable).CreateIfNotExistsAsync()
            let! _ = Async.AwaitTask <| __.TableClient.GetTableReference(config.RuntimeLogsTable).CreateIfNotExistsAsync()
            let! _ = Async.AwaitTask <| __.TableClient.GetTableReference(config.UserDataTable).CreateIfNotExistsAsync()
            let! _ = Async.AwaitTask <| __.BlobClient.GetContainerReference(config.RuntimeContainer).CreateIfNotExistsAsync()
            let! _ = Async.AwaitTask <| __.BlobClient.GetContainerReference(config.UserDataContainer).CreateIfNotExistsAsync()
            ()
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

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Configuration =
    open MBrace.Runtime.Vagabond
    open System.Collections.Generic
    open MBrace.Runtime.Serialization
    open MBrace.Store

    let private ignoredAssemblies = new HashSet<Assembly>()

    let private runOnce (f : unit -> 'T) = let v = lazy(f ()) in fun () -> v.Value

    let private init =
        runOnce(fun () ->
            let _ = System.Threading.ThreadPool.SetMinThreads(100, 100)
            ignoredAssemblies.Add(Assembly.GetExecutingAssembly()) |> ignore
            VagabondRegistry.Initialize(ignoredAssemblies = (ignoredAssemblies |> List.ofSeq), loadPolicy = AssemblyLoadPolicy.ResolveAll))
            
    /// Default Pickler.
    let Pickler = init () ; VagabondRegistry.Instance.Pickler

    /// Default ISerializer
    let Serializer = init (); new FsPicklerBinaryStoreSerializer() :> ISerializer

    /// Initialize Vagabond.
    let Initialize () = init ()

    /// Activates the given configuration.
    let ActivateAsync(config : Configuration) : Async<unit> = 
      async {
        init ()
        let cp = new ClientProvider(config)
        do! cp.InitAll()
        ConfigurationRegistry.Register<ClientProvider>(config.ConfigurationId, cp)
    }

    let AddIgnoredAssembly(asm : Assembly) =
        // MUST BE CALLED BEFORE INIT.
        ignore <| ignoredAssemblies.Add(asm)

    let GetIgnoredAssemblies() : seq<Assembly> =
        ignoredAssemblies :> _

    /// Activates the given configuration.
    let Activate(config : Configuration) = Async.RunSynchronously(ActivateAsync(config))

    /// Warning : Deletes all queues, tables and containers described in the given configuration.
    /// Does not delete process created resources.
    let DeleteResourcesAsync (config : Configuration) =
        async {
            init ()
            let cp = ConfigurationRegistry.Resolve<ClientProvider>(config.ConfigurationId)
            do! cp.ClearRuntimeState()
            do! cp.ClearUserData()
        }