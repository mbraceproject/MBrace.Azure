namespace MBrace.Azure.Client

open MBrace.Azure.Runtime
open MBrace.Continuation
open MBrace.Store
open MBrace.Runtime.Store
open MBrace.Azure.Store

[<Sealed>]
type internal StoreClient private () =
    
    static member CreateDefault(config : Configuration) : ResourceRegistry * MBrace.Store.StoreClient =

        Configuration.Activate(config)

        let inMemoryCache = InMemoryCache.Create(physicalMemoryLimitPercentage = 20)
        let cache = FileSystemStore.CreateUniqueLocal()
        let storeProvider = BlobStore.Create(config.StorageConnectionString) :> ICloudFileStore
        let atomProvider = 
            { new AtomProvider(config.StorageConnectionString, Configuration.Serializer) with
                override __.ComputeSize(value : 'T) = Configuration.Pickler.ComputeSize(value) } :> ICloudAtomProvider
        let channelProvider = ChannelProvider.Create(config.ServiceBusConnectionString, Configuration.Serializer)
    
        let defaultStoreContainer = config.DefaultTableOrContainer
        let defaultAtomContainer = config.DefaultTableOrContainer
        let defaultChannelContainer = ""

        let resources = 
            resource { 
                yield { FileStore = FileStoreCache.Create(storeProvider, cache);
                        DefaultDirectory = defaultStoreContainer;
                        Cache = Some(inMemoryCache :> _);
                        Serializer = Configuration.Serializer
                       }
                yield { AtomProvider = atomProvider; DefaultContainer = defaultAtomContainer }
                yield { ChannelProvider = channelProvider; DefaultContainer = defaultChannelContainer } 
            }

        let sc = MBrace.Store.StoreClient.CreateFromResources(resources)
        resources, sc