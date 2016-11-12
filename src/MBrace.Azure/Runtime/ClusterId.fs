namespace MBrace.Azure.Runtime

open System
open System.Collections.Concurrent
open System.Runtime.Serialization
open System.Text

open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging
open Microsoft.WindowsAzure.Storage

open MBrace.FsPickler

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Azure
open MBrace.Azure.Runtime.Utilities

/// Serializable state/configuration record uniquely identifying an MBrace.Azure cluster
[<AutoSerializable(true); StructuralEquality; StructuralComparison>]
[<StructuredFormatDisplay("{Id}")>]
type ClusterId =
    {
        /// Runtime version string
        Version : string
        /// Azure storage account name.
        StorageAccount : AzureStorageAccount
        /// Service Bus hostname.
        ServiceBusAccount : AzureServiceBusAccount

        /// Service Bus Queue.
        WorkItemQueue : string
        /// Service Bus Topic.
        WorkItemTopic : string

        /// Runtime blob container.
        RuntimeContainer : string
        /// User data container.
        UserDataContainer : string
        /// Vagabond Assembly Container.
        VagabondContainer : string
        /// CloudValue Persist Container.
        CloudValueContainer : string

        /// Runtime table.
        RuntimeTable : string
        /// Runtime logs table.
        RuntimeLogsTable : string
        /// User data table.
        UserDataTable : string

        /// Specifies whether closure serialization
        /// should be optimized using closure sifting.
        OptimizeClosureSerialization : bool
    }

    member this.Id = sprintf "{Storage = \"%s\"; ServiceBus = \"%s\"}" this.StorageAccount.AccountName this.ServiceBusAccount.AccountName
    interface IRuntimeId with member this.Id = this.Id

    member this.Hash = FsPickler.ComputeHash(this).Hash |> Convert.ToBase64String

    member private this.DeleteTable(tableName : string) = async {
        let! _ = this.StorageAccount.TableClient.GetTableReference(tableName).DeleteIfExistsAsync() |> Async.AwaitTaskCorrect
        return ()
    }

    member private this.DeleteContainer(containerName : string) = async {
        let! _ = this.StorageAccount.BlobClient.GetContainerReference(containerName).DeleteIfExistsAsync() |> Async.AwaitTaskCorrect
        return ()
    }

    member this.ClearUserData() = async {
        do!
            [|
                this.DeleteTable this.UserDataTable
                this.DeleteContainer this.UserDataContainer
                this.DeleteContainer this.CloudValueContainer
            |]
            |> Async.Parallel
            |> Async.Ignore
    }

    member this.ClearVagabondData() = async {
        do! this.DeleteContainer this.VagabondContainer
    }

    member this.ClearRuntimeState() = async {
        do!
            [|
                this.DeleteTable this.RuntimeTable
                this.DeleteContainer this.RuntimeContainer
            |]
            |> Async.Parallel
            |> Async.Ignore
    }

    member this.ClearRuntimeLogs() = async {
        do! this.DeleteTable this.RuntimeLogsTable
    }

    member this.ClearRuntimeQueues() = async {
        do!
            [|
                Async.AwaitTaskCorrect(this.ServiceBusAccount.NamespaceManager.DeleteQueueAsync this.WorkItemQueue)
                Async.AwaitTaskCorrect(this.ServiceBusAccount.NamespaceManager.DeleteTopicAsync this.WorkItemTopic)
            |]
            |> Async.Parallel
            |> Async.Ignore
    }

    /// <summary>
    ///   Initializes all store resources on which the current runtime depends.  
    /// </summary>
    /// <param name="maxRetries">Maximum number of retries on conflicts. Defaults to infinite retries.</param>
    /// <param name="retryInterval">Retry sleep interval. Defaults to 3000ms.</param>
    member this.InitializeAllStoreResources(?maxRetries : int, ?retryInterval : int) = async {
        let createTable name = async { do! this.StorageAccount.GetTableReference(name).CreateIfNotExistsAsyncSafe(?maxRetries = maxRetries, ?retryInterval = retryInterval) }
        let createContainer name = async { do! this.StorageAccount.GetContainerReference(name).CreateIfNotExistsAsyncSafe(?maxRetries = maxRetries, ?retryInterval = retryInterval) }
        do!
            [|  
                createTable this.RuntimeTable
                createTable this.UserDataTable
                createTable this.RuntimeLogsTable

                createContainer this.RuntimeContainer
                createContainer this.UserDataContainer
                createContainer this.CloudValueContainer
                createContainer this.VagabondContainer
            |]
            |> Async.Parallel
            |> Async.Ignore
    }

    /// <summary>
    ///     Activates a cluster id instance using provided configuration object.
    /// </summary>
    /// <param name="configuration">Azure cluster configuration object.</param>
    static member Activate(configuration : Configuration) =
        ProcessConfiguration.EnsureInitialized()
        let version = Version.Parse configuration.Version

        let appendVersionAndSuffixId (text : string) =
            sprintf "%s%s%s" text 
                (if configuration.UseVersionSuffix then sprintf "v%dv%d" version.Major version.Minor else "") 
                (if configuration.UseSuffixId then sprintf "s%05d" configuration.SuffixId else "")

        let appendSuffixId (text : string) =
            sprintf "%s%s" text
                (if configuration.UseSuffixId then sprintf "s%05d" configuration.SuffixId else "")

        {
            Version                         = version.ToString(4)
            StorageAccount                  = AzureStorageAccount.FromConnectionString configuration.StorageConnectionString
            ServiceBusAccount               = AzureServiceBusAccount.FromConnectionString configuration.ServiceBusConnectionString

            WorkItemQueue                   = appendVersionAndSuffixId configuration.WorkItemQueue
            WorkItemTopic                   = appendVersionAndSuffixId configuration.WorkItemTopic

            RuntimeContainer                = appendVersionAndSuffixId configuration.RuntimeContainer
            VagabondContainer               = configuration.AssemblyContainer
            CloudValueContainer             = appendVersionAndSuffixId configuration.CloudValueContainer
            UserDataContainer               = appendSuffixId configuration.UserDataContainer

            RuntimeTable                    = appendVersionAndSuffixId configuration.RuntimeTable
            RuntimeLogsTable                = appendVersionAndSuffixId configuration.RuntimeLogsTable
            UserDataTable                   = appendSuffixId configuration.UserDataTable

            OptimizeClosureSerialization    = configuration.OptimizeClosureSerialization
        }


/// Dependency injection facility for Specific cluster instances
[<Sealed;AbstractClass>]
type ConfigurationRegistry private () =
    static let registry = new ConcurrentDictionary<ClusterId * Type, obj>()

    static member Register<'T>(clusterId : ClusterId, item : 'T) : unit =
        ignore <| registry.TryAdd((clusterId, typeof<'T>), item :> obj)

    static member Resolve<'T>(clusterId : ClusterId) : 'T =
        let mutable result = null
        if registry.TryGetValue((clusterId, typeof<'T>), &result) then result :?> 'T
        else
            invalidOp <| sprintf "Could not resolve Resource of type %A for ConfigurationId %A" clusterId typeof<'T>