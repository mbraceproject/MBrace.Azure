namespace MBrace.Azure.Tests.Store

open NUnit.Framework
open MBrace.Core
open MBrace.Core.Tests
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Components
open MBrace.ThreadPool
open MBrace.Azure
open MBrace.Azure.Store
open MBrace.Azure.Tests

// BlobStore tests

[<AbstractClass; TestFixture>]
type ``Azure BlobStore Tests``(config : Configuration, workerCount : int) = 
    inherit ``CloudFileStore Tests``(parallelismFactor = 10)
    let session = new LocalClusterSession(config, workerCount)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.FileStore = session.Cluster.GetResource<ICloudFileStore>()
    override __.Serializer = session.Cluster.GetResource<ISerializer>()
    override __.IsCaseSensitive = false
    override __.Run(workflow : Cloud<'T>) = session.Cluster.Run workflow
    override __.RunOnCurrentProcess(workflow : Cloud<'T>) = session.Cluster.RunOnCurrentProcess workflow

[<TestFixture>]
type ``BlobStore Tests - Standalone Cluster - Remote Storage``() =
    inherit ``Azure BlobStore Tests``(remoteConfig, 4)

[<TestFixture>]
type ``BlobStore Tests - Standalone Cluster - Storage Emulator``() = 
    inherit ``Azure BlobStore Tests``(emulatorConfig, 4)

[<TestFixture>]
type ``BlobStore Tests - Remote Cluster - Remote Storage``() = 
    inherit ``Azure BlobStore Tests``(remoteConfig, 0)


// CloudAtom tests


[<AbstractClass; TestFixture>]
type ``Azure CloudAtom Tests``(config : Configuration, workerCount : int) = 
    inherit ``CloudAtom Tests``(parallelismFactor = 5)
    let session = new LocalClusterSession(config, workerCount)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.IsSupportedNamedLookup = true
    override __.Run wf = session.Cluster.Run wf
    override __.RunOnCurrentProcess wf = session.Cluster.RunOnCurrentProcess wf
    override __.Repeats = 1

[<TestFixture>]
type ``CloudAtom Tests - Standalone Cluster - Remote Storage``() = 
    inherit ``Azure CloudAtom Tests``(remoteConfig, 4)

[<TestFixture>]
type ``CloudAtom Tests - Standalone Cluster - Storage Emulator``() = 
    inherit ``Azure CloudAtom Tests``(emulatorConfig, 4)
    override __.Repeats = 3

[<TestFixture>]
type ``CloudAtom Tests - Remote Cluster - Remote Storage``() = 
    inherit ``Azure CloudAtom Tests``(remoteConfig, 0)


// CloudQueue Tests


[<AbstractClass; TestFixture>]
type ``Azure CloudQueue Tests``(config : Configuration, workerCount : int) = 
    inherit ``CloudQueue Tests``(parallelismFactor = 10)
    let session = new LocalClusterSession(config, workerCount)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.Run wf = session.Cluster.Run wf
    override __.RunOnCurrentProcess wf = session.Cluster.RunOnCurrentProcess wf
    override __.IsSupportedNamedLookup = true

[<TestFixture>]
type ``CloudQueue Tests - Standalone Cluster - Remote Storage``() = 
    inherit ``Azure CloudQueue Tests``(remoteConfig, 4)

[<TestFixture>]
type ``CloudQueue Tests - Remote Cluster - Remote Storage``() = 
    inherit ``Azure CloudQueue Tests``(remoteConfig, 0)


// CloudDictionary tests


[<AbstractClass; TestFixture>]
type ``Azure CloudDictionary Tests``(config : Configuration, workerCount : int) = 
    inherit ``CloudDictionary Tests``(parallelismFactor = 5)
    let session = new LocalClusterSession(emulatorConfig, 4)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.Run wf = session.Cluster.Run wf
    override __.RunOnCurrentProcess wf = session.Cluster.RunOnCurrentProcess wf
    override __.IsInMemoryFixture = false
    override __.IsSupportedNamedLookup = true

[<TestFixture>]
type ``CloudDictionary Tests - Standalone Cluster - Storage Emulator``() = 
    inherit ``Azure CloudDictionary Tests``(emulatorConfig, 4)

[<TestFixture>]
type ``CloudDictionary Tests - Standalone Cluster - Remote Storage``() = 
    inherit ``Azure CloudDictionary Tests``(remoteConfig, 4)

[<TestFixture>]
type ``CloudDictionary Tests - Remote Cluster - Remote Storage``() = 
    inherit ``Azure CloudDictionary Tests``(remoteConfig, 0)


// CloudValue tests


[<AbstractClass; TestFixture>]
type ``Azure CloudValue Tests``(config : Configuration, workerCount : int) = 
    inherit ``CloudValue Tests``(parallelismFactor = 5)
    let session = new LocalClusterSession(emulatorConfig, 4)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.Run wf = session.Cluster.Run wf
    override __.RunOnCurrentProcess wf = session.Cluster.RunOnCurrentProcess wf
    override __.IsSupportedLevel _ = true

[<TestFixture>]
type ``CloudValue Tests - Standalone Cluster - Storage Emulator``() = 
    inherit ``Azure CloudValue Tests``(emulatorConfig, 4)

[<TestFixture>]
type ``CloudValue Tests - Standalone Cluster - Remote Storage``() = 
    inherit ``Azure CloudValue Tests``(remoteConfig, 4)

[<TestFixture>]
type ``CloudValue Tests - Remote Cluster - Remote Storage``() = 
    inherit ``Azure CloudValue Tests``(remoteConfig, 0)