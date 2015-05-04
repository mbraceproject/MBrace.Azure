namespace MBrace.Azure.Client

open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals
open MBrace.Runtime.Store
open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Store

[<Sealed>]
type internal StoreClient private () =
    
    static member CreateDefault(config : Configuration) : ResourceRegistry * MBrace.Client.CloudStoreClient =
        let storeProvider = BlobStore.Create(config.StorageConnectionString) :> ICloudFileStore
        let atomProvider = AtomProvider.Create(config.StorageConnectionString) :> ICloudAtomProvider
        let channelProvider = ChannelProvider.Create(config.ServiceBusConnectionString) :> ICloudChannelProvider
        let dictionaryProvider = CloudDictionaryProvider.Create(config.StorageConnectionString) :> ICloudDictionaryProvider
        
        let defaultStoreContainer = config.UserDataContainer
        let defaultAtomContainer = config.UserDataTable
        let defaultChannelContainer = ""

        let resources = 
            resource { 
                yield Configuration.Serializer
                yield CloudFileStoreConfiguration.Create(storeProvider, defaultStoreContainer)
                yield { AtomProvider = atomProvider
                        DefaultContainer = defaultAtomContainer }
                yield { ChannelProvider = channelProvider
                        DefaultContainer = defaultChannelContainer } 
                yield dictionaryProvider
            }

        let sc = MBrace.Client.CloudStoreClient.CreateFromResources(resources)
        resources, sc