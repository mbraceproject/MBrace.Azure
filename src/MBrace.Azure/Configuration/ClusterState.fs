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

/// Serializable state/configuration record uniquely identifying an MBrace.Azure cluster
[<AutoSerializable(true); StructuralEquality; StructuralComparison>]
[<StructuredFormatDisplay("{Id}")>]
type ClusterState =
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

    member this.ClearUserData() = async {
        do! this.StorageAccount.GetTableReference(this.UserDataTable).DeleteIfExistsAsync()
        do! this.StorageAccount.GetContainerReference(this.UserDataContainer).DeleteIfExistsAsync()
        do! this.StorageAccount.GetContainerReference(this.CloudValueContainer).DeleteIfExistsAsync()
    }

    member this.ClearVagabondData() = async {
        do! this.StorageAccount.GetContainerReference(this.VagabondContainer).DeleteIfExistsAsync()
    }

    member this.ClearRuntimeState() = async {
        do! this.StorageAccount.GetTableReference(this.RuntimeTable).DeleteIfExistsAsync()
        do! this.StorageAccount.GetContainerReference(this.RuntimeContainer).DeleteIfExistsAsync()
    }

    member this.ClearRuntimeLogs() = async {
        do! this.StorageAccount.GetTableReference(this.RuntimeLogsTable).DeleteIfExistsAsync()
    }

    member this.ClearRuntimeQueues() = async {
        do! this.ServiceBusAccount.NamespaceManager.DeleteQueueAsync this.RuntimeQueue
        do! this.ServiceBusAccount.NamespaceManager.DeleteTopicAsync this.RuntimeTopic
    }

    member this.InitializeAll() = async {
        let aat t = Async.AwaitTask t
        do!
            [|  aat <| this.StorageAccount.GetTableReference(this.RuntimeTable).CreateIfNotExistsAsync()
                aat <| this.StorageAccount.GetTableReference(this.UserDataTable).CreateIfNotExistsAsync()
                aat <| this.StorageAccount.GetTableReference(this.RuntimeLogsTable).CreateIfNotExistsAsync()

                aat <| this.StorageAccount.GetTableReference(this.RuntimeContainer).CreateIfNotExistsAsync()
                aat <| this.StorageAccount.GetTableReference(this.UserDataContainer).CreateIfNotExistsAsync()
                aat <| this.StorageAccount.GetTableReference(this.CloudValueContainer).CreateIfNotExistsAsync()
                aat <| this.StorageAccount.GetTableReference(this.VagabondContainer).CreateIfNotExistsAsync() |]
     
            |> Async.Parallel
            |> Async.Ignore
    }

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
    static let registry = new ConcurrentDictionary<ClusterState * Type, obj>()

    static member Register<'T>(config : ClusterState, item : 'T) : unit =
        registry.TryAdd((config, typeof<'T>), item :> obj)
        |> ignore

    static member Resolve<'T>(config : ClusterState) : 'T =
        match registry.TryGetValue((config, typeof<'T>)) with
        | true, v  -> v :?> 'T
        | false, _ -> invalidOp <| sprintf "Could not resolve Resource of type %A for ConfigurationId %A" config typeof<'T>