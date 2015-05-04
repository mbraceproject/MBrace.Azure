namespace MBrace.Azure.Store.Tests

open NUnit.Framework

open MBrace.Core.Tests
open MBrace.Store.Internals
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
             CloudFileStoreConfiguration.Create(store)

    let emulatorBlobStoreConfig = 
        lazy let store = BlobStore.Create(emulatorConn)
             CloudFileStoreConfiguration.Create(store)

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

    let emulatorDictionaryProvider =
        lazy (CloudDictionaryProvider.Create(emulatorConn) :> ICloudDictionaryProvider)

    let remoteDictionaryProvider =
        lazy (CloudDictionaryProvider.Create(remoteConn.Value) :> ICloudDictionaryProvider)

[<TestFixture>]
type ``Remote - BlobStore Tests`` () =
    inherit  ``Local FileStore Tests``(remoteBlobStoreConfig.Value, serializer)

[<TestFixture>]
type ``Emulator - BlobStore Tests`` () =
    inherit  ``Local FileStore Tests``(emulatorBlobStoreConfig.Value, serializer)

[<TestFixture>]
type ``Remote - Atom Tests`` () =
    inherit ``CloudAtom Tests``(5)

    let imem = LocalRuntime.Create(atomConfig = remoteAtomStoreConfig.Value)

    override __.Run wf = imem.Run wf
    override __.RunLocally wf = imem.Run wf
    override __.AtomClient = imem.StoreClient.Atom
    override __.Repeats = 1

[<TestFixture>]
type ``Emulator - Atom Tests`` () =
    inherit ``CloudAtom Tests``(5)

    let imem = LocalRuntime.Create(atomConfig = emulatorAtomStoreConfig.Value)

    override __.Run wf = imem.Run wf
    override __.RunLocally wf = imem.Run wf
    override __.AtomClient = imem.StoreClient.Atom
    override __.Repeats = 3



[<TestFixture>]
type ``Remote - Channel Tests`` () =
    inherit ``CloudChannel Tests``(10) 
    
    let imem = LocalRuntime.Create(channelConfig = remoteChannelStoreConfig.Value)

    override __.Run wf = imem.Run wf
    override __.RunLocally wf = imem.Run wf
    override __.ChannelClient = imem.StoreClient.Channel


[<TestFixture>]
type ``Emulator - Dictionary Tests`` () =
    inherit ``CloudDictionary Tests``(5)

    let imem = LocalRuntime.Create(dictionaryProvider = emulatorDictionaryProvider.Value)

    override __.Run wf = imem.Run wf
    override __.RunLocally wf = imem.Run wf
    override __.DictionaryClient = imem.StoreClient.Dictionary
    override __.IsInMemoryFixture = false


[<TestFixture>]
type ``Remote - Dictionary Tests`` () =
    inherit ``CloudDictionary Tests``(5) 
    
    let imem = LocalRuntime.Create(dictionaryProvider = remoteDictionaryProvider.Value)

    override __.Run wf = imem.Run wf
    override __.RunLocally wf = imem.Run wf
    override __.DictionaryClient = imem.StoreClient.Dictionary
    override __.IsInMemoryFixture = false
