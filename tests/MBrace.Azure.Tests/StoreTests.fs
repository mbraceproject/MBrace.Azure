namespace MBrace.Azure.Tests.Store

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Tests
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.ThreadPool
open MBrace.Azure.Store
open MBrace.Azure.Tests

[<AutoOpen>]
module private Config =

    do VagabondRegistry.Initialize(throwOnError = false)

    let serializer = new VagabondFsPicklerBinarySerializer()

    let emulatorConn = "UseDevelopmentStorage=true"
    let remoteConn = lazy Utils.selectEnv "azurestorageconn"

    let remoteBlobStoreConfig = lazy BlobStore.Create(remoteConn.Value, "mbracetests")

    let emulatorBlobStoreConfig = lazy BlobStore.Create(emulatorConn, "mbracetests")

    let remoteAtomStoreConfig = lazy AtomProvider.Create(remoteConn.Value, "mbraceTest")

    let emulatorAtomStoreConfig = lazy AtomProvider.Create(emulatorConn, "mbraceTest")

    let remoteChannelStoreConfig = lazy QueueProvider.Create(Utils.selectEnv "azureservicebusconn")

    let emulatorDictionaryProvider = lazy CloudDictionaryProvider.Create(emulatorConn)

    let remoteDictionaryProvider = lazy CloudDictionaryProvider.Create(remoteConn.Value)

[<TestFixture>]
type ``Remote - BlobStore Tests`` () =
    inherit  ``CloudFileStore Tests``(parallelismFactor = 20)

    let store = remoteBlobStoreConfig.Value
    let runtime = ThreadPoolRuntime.Create(fileStore = store, serializer = serializer)

    override __.FileStore = store :> _
    override __.Serializer = serializer :> _
    override __.RunOnCloud (workflow : Cloud<'T>) = runtime.RunSynchronously workflow
    override __.RunOnCurrentProcess (workflow : Cloud<'T>) = runtime.RunSynchronously workflow

[<TestFixture>]
type ``Emulator - BlobStore Tests`` () =
    inherit  ``CloudFileStore Tests``(parallelismFactor = 20)

    let store = emulatorBlobStoreConfig.Value
    let runtime = ThreadPoolRuntime.Create(fileStore = store, serializer = serializer)

    override __.FileStore = store :> _
    override __.Serializer = serializer :> _
    override __.RunOnCloud (workflow : Cloud<'T>) = runtime.RunSynchronously workflow
    override __.RunOnCurrentProcess (workflow : Cloud<'T>) = runtime.RunSynchronously workflow


[<TestFixture>]
type ``Remote - Atom Tests`` () =
    inherit ``CloudAtom Tests``(parallelismFactor = 5)

    let imem = ThreadPoolRuntime.Create(atomProvider = remoteAtomStoreConfig.Value)

    override __.RunOnCloud wf = imem.RunSynchronously wf
    override __.RunOnCurrentProcess wf = imem.RunSynchronously wf
    override __.Repeats = 1

[<TestFixture>]
type ``Emulator - Atom Tests`` () =
    inherit ``CloudAtom Tests``(parallelismFactor = 5)

    let imem = ThreadPoolRuntime.Create(atomProvider = emulatorAtomStoreConfig.Value)

    override __.RunOnCloud wf = imem.RunSynchronously wf
    override __.RunOnCurrentProcess wf = imem.RunSynchronously wf
    override __.Repeats = 3



[<TestFixture>]
type ``Remote - Queue Tests`` () =
    inherit ``CloudQueue Tests``(parallelismFactor = 10) 
    
    let imem = ThreadPoolRuntime.Create(queueProvider = remoteChannelStoreConfig.Value)

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
