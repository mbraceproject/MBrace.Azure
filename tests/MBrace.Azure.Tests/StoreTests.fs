namespace MBrace.Azure.Tests.Store

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Tests
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Components
open MBrace.ThreadPool
open MBrace.Azure.Store
open MBrace.Azure.Tests

[<AutoOpen>]
module private Config =

    do VagabondRegistry.Initialize(throwOnError = false)

    let serializer = new VagabondFsPicklerBinarySerializer()

    let emulatorConn = "UseDevelopmentStorage=true"
    let remoteConn = lazy Utils.selectEnv "azurestorageconn"

    let remoteBlobStore = lazy BlobStore.Create(remoteConn.Value, "mbracetests")

    let emulatorBlobStore = lazy BlobStore.Create(emulatorConn, "mbracetests")

    let remoteAtomStoreProvider = lazy AtomProvider.Create(remoteConn.Value, "mbraceTest")

    let emulatorAtomStoreProvider = lazy AtomProvider.Create(emulatorConn, "mbraceTest")

    let remoteChannelStoreProvider = lazy QueueProvider.Create(Utils.selectEnv "azureservicebusconn")

    let emulatorDictionaryProvider = lazy CloudDictionaryProvider.Create(emulatorConn)

    let remoteDictionaryProvider = lazy CloudDictionaryProvider.Create(remoteConn.Value)

    let emulatorCloudValueProvider = lazy StoreCloudValueProvider.InitCloudValueProvider(emulatorBlobStore.Value)

    let remoteCloudValueProvider = lazy StoreCloudValueProvider.InitCloudValueProvider(remoteBlobStore.Value)

[<TestFixture>]
type ``Remote - BlobStore Tests`` () =
    inherit  ``CloudFileStore Tests``(parallelismFactor = 20)

    let store = remoteBlobStore.Value
    let runtime = ThreadPoolRuntime.Create(fileStore = store, serializer = serializer)

    override __.FileStore = store :> _
    override __.Serializer = serializer :> _
    override __.RunOnCloud (workflow : Cloud<'T>) = runtime.RunSynchronously workflow
    override __.RunOnCurrentProcess (workflow : Cloud<'T>) = runtime.RunSynchronously workflow

[<TestFixture>]
type ``Emulator - BlobStore Tests`` () =
    inherit  ``CloudFileStore Tests``(parallelismFactor = 20)

    let store = emulatorBlobStore.Value
    let runtime = ThreadPoolRuntime.Create(fileStore = store, serializer = serializer)

    override __.FileStore = store :> _
    override __.Serializer = serializer :> _
    override __.RunOnCloud (workflow : Cloud<'T>) = runtime.RunSynchronously workflow
    override __.RunOnCurrentProcess (workflow : Cloud<'T>) = runtime.RunSynchronously workflow


[<TestFixture>]
type ``Remote - Atom Tests`` () =
    inherit ``CloudAtom Tests``(parallelismFactor = 5)

    let imem = ThreadPoolRuntime.Create(atomProvider = remoteAtomStoreProvider.Value)

    override __.RunOnCloud wf = imem.RunSynchronously wf
    override __.RunOnCurrentProcess wf = imem.RunSynchronously wf
    override __.Repeats = 1

[<TestFixture>]
type ``Emulator - Atom Tests`` () =
    inherit ``CloudAtom Tests``(parallelismFactor = 5)

    let imem = ThreadPoolRuntime.Create(atomProvider = emulatorAtomStoreProvider.Value)

    override __.RunOnCloud wf = imem.RunSynchronously wf
    override __.RunOnCurrentProcess wf = imem.RunSynchronously wf
    override __.Repeats = 3



[<TestFixture>]
type ``Remote - Queue Tests`` () =
    inherit ``CloudQueue Tests``(parallelismFactor = 10) 
    
    let imem = ThreadPoolRuntime.Create(queueProvider = remoteChannelStoreProvider.Value)

    override __.RunOnCloud wf = imem.RunSynchronously wf
    override __.RunOnCurrentProcess wf = imem.RunSynchronously wf


[<TestFixture>]
type ``Emulator - Dictionary Tests`` () =
    inherit ``CloudDictionary Tests``(parallelismFactor = 5)

    let imem = ThreadPoolRuntime.Create(dictionaryProvider = emulatorDictionaryProvider.Value)

    override __.RunOnCloud wf = imem.RunSynchronously wf
    override __.RunOnCurrentProcess wf = imem.RunSynchronously wf
    override __.IsInMemoryFixture = false


[<TestFixture>]
type ``Remote - Dictionary Tests`` () =
    inherit ``CloudDictionary Tests``(parallelismFactor = 5) 
    
    let imem = ThreadPoolRuntime.Create(dictionaryProvider = remoteDictionaryProvider.Value)

    override __.RunOnCloud wf = imem.RunSynchronously wf
    override __.RunOnCurrentProcess wf = imem.RunSynchronously wf
    override __.IsInMemoryFixture = false

[<TestFixture>]
type ``Emulator - CloudValue Tests`` () =
    inherit ``CloudValue Tests``(parallelismFactor = 5)

    let imem = ThreadPoolRuntime.Create(dictionaryProvider = emulatorDictionaryProvider.Value)

    override __.RunOnCloud wf = imem.RunSynchronously wf
    override __.RunOnCurrentProcess wf = imem.RunSynchronously wf
    override __.IsSupportedLevel _ = true


[<TestFixture>]
type ``Remote - CloudValue Tests`` () =
    inherit ``CloudValue Tests``(parallelismFactor = 5) 
    
    let imem = ThreadPoolRuntime.Create(valueProvider = remoteCloudValueProvider.Value)

    override __.RunOnCloud wf = imem.RunSynchronously wf
    override __.RunOnCurrentProcess wf = imem.RunSynchronously wf
    override __.IsSupportedLevel _ = true