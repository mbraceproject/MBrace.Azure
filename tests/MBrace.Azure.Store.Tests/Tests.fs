namespace MBrace.Azure.Store.Tests

open MBrace.Tests
open NUnit.Framework
open MBrace.Client

[<AutoOpen>]
module private Config =
    open MBrace.Runtime.Vagabond
    open MBrace.Azure.Store
    open MBrace.Runtime.Serialization
    open MBrace.Store

    do VagabondRegistry.Initialize(throwOnError = false)

    let serializer = MBrace.Runtime.Serialization.FsPicklerBinaryStoreSerializer()

    let emulatorConn = "UseDevelopmentStorage=true"
    let remoteConn = lazy Tests.Utils.selectEnv "azurestorageconn"

    let remoteBlobStoreConfig = 
        lazy let store = BlobStore.Create(remoteConn.Value)
             CloudFileStoreConfiguration.Create(store, serializer)

    let emulatorBlobStoreConfig = 
        lazy let store = BlobStore.Create(emulatorConn)
             CloudFileStoreConfiguration.Create(store, serializer)

    let remoteAtomStoreConfig =
        lazy 
            let store = AtomProvider.Create(remoteConn.Value) :> ICloudAtomProvider 
            in CloudAtomConfiguration.Create(store, "mbracetest")

    let emulatorAtomStoreConfig =
        lazy 
            let store = AtomProvider.Create(emulatorConn) :> ICloudAtomProvider 
            in CloudAtomConfiguration.Create(store, "mbracetest")

    let remoteChannelStoreConfig =
        lazy 
            let store = ChannelProvider.Create(Tests.Utils.selectEnv "azureservicebusconn") :> ICloudChannelProvider
            in CloudChannelConfiguration.Create(store)


[<TestFixture>]
type ``Remote - BlobStore Tests`` () =
    inherit  ``Local FileStore Tests``({ remoteBlobStoreConfig.Value with Cache = None })
    override __.IsCachingStore = false

[<TestFixture>]
type ``Emulator - BlobStore Tests`` () =
    inherit  ``Local FileStore Tests``({ emulatorBlobStoreConfig.Value with Cache = None })
    override __.IsCachingStore = false




[<TestFixture>]
type ``Remote - Atom Tests`` () =
    inherit ``CloudAtom Tests``(5)

    let imem = LocalRuntime.Create(atomConfig = remoteAtomStoreConfig.Value)

    override __.Run wf = imem.Run wf
    override __.RunLocal wf = imem.Run wf
    override __.AtomClient = imem.StoreClient.Atom
    override __.Repeats = 1

[<TestFixture>]
type ``Emulator - Atom Tests`` () =
    inherit ``CloudAtom Tests``(5)

    let imem = LocalRuntime.Create(atomConfig = emulatorAtomStoreConfig.Value)

    override __.Run wf = imem.Run wf
    override __.RunLocal wf = imem.Run wf
    override __.AtomClient = imem.StoreClient.Atom
    override __.Repeats = 3



[<TestFixture>]
type ``Remote - Channel Tests`` () =
    inherit ``CloudChannel Tests``(10) 
    
    let imem = LocalRuntime.Create(channelConfig = remoteChannelStoreConfig.Value)

    override __.Run wf = imem.Run wf
    override __.RunLocal wf = imem.Run wf
    override __.ChannelClient = imem.StoreClient.Channel

