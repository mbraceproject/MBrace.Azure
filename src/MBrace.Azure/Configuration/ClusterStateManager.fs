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

/// Configuration record uniquely identifying an MBrace.Azure cluster
[<AutoSerializable(true); StructuralEquality; StructuralComparison>]
type ClusterConfiguration =
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

/// Serializable Cluster
[<Sealed; DataContract; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type ClusterStateManager private (config : ClusterConfiguration) =

    [<DataMember(Name = "Configuration")>]    
    let config = config

    member __.Config = config
    member __.StorageAccount = config.StorageAccount
    member __.ServiceBusAccount = config.ServiceBusAccount
    member __.NamespaceClient = config.ServiceBusAccount.NamespaceManager
    member __.TableClient = config.StorageAccount.TableClient
    member __.BlobClient = config.StorageAccount.BlobClient

    member this.CreateQueueClient(queue : string, mode : ReceiveMode) = QueueClient.CreateFromConnectionString(config.ServiceBusAccount.ConnectionString, queue, mode)
    member this.CreateSubscriptionClient(topic : string, name : string) = SubscriptionClient.CreateFromConnectionString(config.ServiceBusAccount.ConnectionString, topic, name)
    member this.CreateTopicClient(topic : string) = TopicClient.CreateFromConnectionString(config.ServiceBusAccount.ConnectionString, topic)

    member __.RuntimeQueue(mode : ReceiveMode) = __.CreateQueueClient(config.RuntimeQueue, mode)
    member __.RuntimeTopic = __.CreateTopicClient(config.RuntimeTopic)

    member __.RuntimeContainer = config.StorageAccount.BlobClient.GetContainerReference config.RuntimeContainer
    member __.UserDataContainer = config.StorageAccount.BlobClient.GetContainerReference config.UserDataContainer
    member __.VagabondContainer = config.StorageAccount.BlobClient.GetContainerReference config.VagabondContainer
    member __.CloudValueContainer = config.StorageAccount.BlobClient.GetContainerReference config.CloudValueContainer

    member __.RuntimeTable = config.StorageAccount.TableClient.GetTableReference config.RuntimeTable
    member __.RuntimeLogsTable = config.StorageAccount.TableClient.GetTableReference config.RuntimeLogsTable
    member __.UserDataTable = config.StorageAccount.TableClient.GetTableReference config.UserDataTable

    interface IRuntimeId with
        member __.Id = 
            let hash = ProcessConfiguration.Serializer.ComputeHash config
            sprintf "AzureCluster-%s" <| Convert.ToBase64String hash.Hash

    member private __.StructuredFormatDisplay = sprintf "%A" config
    override __.ToString() = sprintf "%A" config

    interface IComparable with
        member __.CompareTo(other:obj) =
            match other with
            | :? ClusterStateManager as cc' -> compare config cc'.Config
            | _ -> invalidArg "other" "invalid comparand."

    override __.Equals(other : obj) =
        match other with
        | :? ClusterStateManager as cc' -> config = cc'.Config
        | _ -> false

    override __.GetHashCode() = config.GetHashCode()

    member this.ClearUserData() = async {
        do! this.UserDataTable.DeleteIfExistsAsync()
        do! this.UserDataContainer.DeleteIfExistsAsync()
        do! this.CloudValueContainer.DeleteIfExistsAsync()
    }

    member this.ClearVagabondData() = async {
        do! this.VagabondContainer.DeleteIfExistsAsync()
    }

    member this.ClearRuntimeState() = async {
        do! this.RuntimeTable.DeleteIfExistsAsync()
        do! this.RuntimeContainer.DeleteIfExistsAsync()
    }

    member this.ClearRuntimeLogs() = async {
        do! this.RuntimeLogsTable.DeleteIfExistsAsync()
    }

    member this.ClearRuntimeQueues() = async {
        do! this.NamespaceClient.DeleteQueueAsync config.RuntimeQueue
        do! this.NamespaceClient.DeleteTopicAsync config.RuntimeTopic
    }

    member this.InitAll() = async {
        let aat t = Async.AwaitTask t
        do!
            [|  aat <| this.RuntimeTable.CreateIfNotExistsAsync()
                aat <| this.RuntimeLogsTable.CreateIfNotExistsAsync()
                aat <| this.UserDataTable.CreateIfNotExistsAsync()
                aat <| this.RuntimeContainer.CreateIfNotExistsAsync()
                aat <| this.UserDataContainer.CreateIfNotExistsAsync()
                aat <| this.CloudValueContainer.CreateIfNotExistsAsync()
                aat <| this.VagabondContainer.CreateIfNotExistsAsync()
                aat <| this.NamespaceClient.QueueExistsAsync(config.RuntimeQueue) |]
     
            |> Async.Parallel
            |> Async.Ignore
    }

    static member Activate(configuration : Configuration) =
        ProcessConfiguration.EnsureInitialized()
        let version = Version.Parse configuration.Version
        let versionPostfix = sprintf "%dx%dx" version.Major version.Minor

        let appendVersionAndId (text : string) =
            sprintf "%s%s%s" text 
                (if configuration.UseVersionPostfix then versionPostfix else "") 
                (if configuration.UseSuffixId then sprintf "%05d" configuration.SuffixId else "")

        let config = {
            Version                        = version.ToString(4)
            StorageAccount                 = configuration.StorageAccount
            ServiceBusAccount              = configuration.ServiceBusAccount

            RuntimeQueue                   = appendVersionAndId configuration.RuntimeQueue
            RuntimeTopic                   = appendVersionAndId configuration.RuntimeTopic

            RuntimeContainer               = appendVersionAndId configuration.RuntimeContainer
            VagabondContainer              = appendVersionAndId configuration.AssemblyContainer
            CloudValueContainer            = appendVersionAndId configuration.CloudValueContainer
            UserDataContainer              = appendVersionAndId configuration.UserDataContainer

            RuntimeTable                   = appendVersionAndId configuration.RuntimeTable
            RuntimeLogsTable               = appendVersionAndId configuration.RuntimeLogsTable
            UserDataTable                  = appendVersionAndId configuration.UserDataTable
        }

        new ClusterStateManager(config)