namespace MBrace.Azure.Runtime

open System
open System.Collections.Concurrent
open System.Runtime.Serialization
open System.Text

open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging
open Microsoft.WindowsAzure.Storage

open Nessos.FsPickler

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
        RuntimeQueue : string
        /// Service Bus Topic.
        RuntimeTopic : string

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
    }
with

    member this.Id =
        let hash = ProcessConfiguration.Serializer.ComputeHash this
        sprintf "AzureCluster-%s" <| Convert.ToBase64String hash.Hash
                    
    interface IRuntimeId with
        member this.Id = this.Id

    member private this.DeleteTable(tableName : string) = async {
        let! _ = this.StorageAccount.TableClient.GetTableReference(tableName).DeleteIfExistsAsync()
        return ()
    }

    member private this.DeleteContainer(containerName : string) = async {
        let! _ = this.StorageAccount.BlobClient.GetContainerReference(containerName).DeleteIfExistsAsync()
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
                Async.AwaitTaskCorrect(this.ServiceBusAccount.NamespaceManager.DeleteQueueAsync this.RuntimeQueue)
                Async.AwaitTaskCorrect(this.ServiceBusAccount.NamespaceManager.DeleteTopicAsync this.RuntimeTopic)
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
        let versionPostfix = sprintf "%dx%dx" version.Major version.Minor

        let appendVersionAndId (text : string) =
            sprintf "%s%s%s" text 
                (if configuration.UseVersionSuffix then versionPostfix else "") 
                (if configuration.UseSuffixId then sprintf "%05d" configuration.SuffixId else "")

        {
            Version                 = version.ToString(4)
            StorageAccount          = AzureStorageAccount.Parse configuration.StorageConnectionString
            ServiceBusAccount       = AzureServiceBusAccount.Parse configuration.ServiceBusConnectionString

            RuntimeQueue            = appendVersionAndId configuration.RuntimeQueue
            RuntimeTopic            = appendVersionAndId configuration.RuntimeTopic

            RuntimeContainer        = appendVersionAndId configuration.RuntimeContainer
            VagabondContainer       = appendVersionAndId configuration.AssemblyContainer
            CloudValueContainer     = appendVersionAndId configuration.CloudValueContainer
            UserDataContainer       = appendVersionAndId configuration.UserDataContainer

            RuntimeTable            = appendVersionAndId configuration.RuntimeTable
            RuntimeLogsTable        = appendVersionAndId configuration.RuntimeLogsTable
            UserDataTable           = appendVersionAndId configuration.UserDataTable
        }


/// Dependency injection facility for Specific cluster instances
[<Sealed;AbstractClass>]
type ConfigurationRegistry private () =
    static let registry = new ConcurrentDictionary<ClusterId * Type, obj>()

    static member Register<'T>(config : ClusterId, item : 'T) : unit =
        registry.TryAdd((config, typeof<'T>), item :> obj)
        |> ignore

    static member Resolve<'T>(config : ClusterId) : 'T =
        let mutable result = null
        if registry.TryGetValue((config, typeof<'T>), &result) then result :?> 'T
        else
            invalidOp <| sprintf "Could not resolve Resource of type %A for ConfigurationId %A" config typeof<'T>