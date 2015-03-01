namespace MBrace.Azure.Client

open MBrace.Azure.Runtime
open MBrace.Continuation
open MBrace.Store
open MBrace.Runtime.Store
open MBrace.Azure.Store
open MBrace.Azure

[<Sealed>]
type internal StoreClient private () =
    
    static member CreateDefault(config : Configuration) : ResourceRegistry * MBrace.Client.StoreClient =
        let storeProvider = BlobStore.Create(config.StorageConnectionString) :> ICloudFileStore
        let atomProvider = AtomProvider.Create(config.StorageConnectionString) :> ICloudAtomProvider
        let channelProvider = ChannelProvider.Create(config.ServiceBusConnectionString) :> ICloudChannelProvider
    
        let defaultStoreContainer = config.UserDataContainer
        let defaultAtomContainer = config.UserDataTable
        let defaultChannelContainer = ""

        let resources = 
            resource { 
                yield { FileStore = storeProvider
                        DefaultDirectory = defaultStoreContainer
                        Cache = None
                        Serializer = Configuration.Serializer }
                yield { AtomProvider = atomProvider
                        DefaultContainer = defaultAtomContainer }
                yield { ChannelProvider = channelProvider
                        DefaultContainer = defaultChannelContainer } 
            }

        let sc = MBrace.Client.StoreClient.CreateFromResources(resources)
        resources, sc