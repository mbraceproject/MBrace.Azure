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
    
[<TestFixture>]
type ``BlobStore Tests - Standalone Cluster - Remote Storage``() = 
    inherit ``CloudFileStore Tests``(parallelismFactor = 10)
    let session = new ClusterSession(remoteConfig, 4)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.FileStore = session.cluster.GetResource<ICloudFileStore>()
    override __.Serializer = session.cluster.GetResource<ISerializer>()
    override __.IsCaseSensitive = false
    override __.Run(workflow : Cloud<'T>) = session.cluster.Run workflow
    override __.RunOnCurrentProcess(workflow : Cloud<'T>) = session.cluster.RunOnCurrentProcess workflow

[<TestFixture>]
type ``BlobStore Tests - Standalone Cluster - Storage Emulator``() = 
    inherit ``CloudFileStore Tests``(parallelismFactor = 10)
    let session = new ClusterSession(emulatorConfig, 4)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.FileStore = session.cluster.GetResource<ICloudFileStore>()
    override __.Serializer = session.cluster.GetResource<ISerializer>()
    override __.IsCaseSensitive = false
    override __.Run(workflow : Cloud<'T>) = session.cluster.Run workflow
    override __.RunOnCurrentProcess(workflow : Cloud<'T>) = session.cluster.RunOnCurrentProcess workflow

[<TestFixture>]
type ``Atom Tests - Standalone Cluster - Remote Storage``() = 
    inherit ``CloudAtom Tests``(parallelismFactor = 5)
    let session = new ClusterSession(remoteConfig, 4)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.Run wf = session.cluster.Run wf
    override __.RunOnCurrentProcess wf = session.cluster.RunOnCurrentProcess wf
    override __.Repeats = 1

[<TestFixture>]
type ``Atom Tests - Standalone Cluster - Storage Emulator``() = 
    inherit ``CloudAtom Tests``(parallelismFactor = 5)
    let session = new ClusterSession(emulatorConfig, 4)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.Run wf = session.cluster.Run wf
    override __.RunOnCurrentProcess wf = session.cluster.RunOnCurrentProcess wf
    override __.Repeats = 3

[<TestFixture>]
type ``Queue Tests - Standalone Cluster - Remote Storage``() = 
    inherit ``CloudQueue Tests``(parallelismFactor = 10)
    let session = new ClusterSession(remoteConfig, 4)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.Run wf = session.cluster.Run wf
    override __.RunOnCurrentProcess wf = session.cluster.RunOnCurrentProcess wf

[<TestFixture>]
type ``Dictionary Tests - Standalone Cluster - Storage Emulator``() = 
    inherit ``CloudDictionary Tests``(parallelismFactor = 5)
    let session = new ClusterSession(emulatorConfig, 4)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.Run wf = session.cluster.Run wf
    override __.RunOnCurrentProcess wf = session.cluster.RunOnCurrentProcess wf
    override __.IsInMemoryFixture = false

[<TestFixture>]
type ``Dictionary Tests - Standalone Cluster - Remote Storage``() = 
    inherit ``CloudDictionary Tests``(parallelismFactor = 5)
    let session = new ClusterSession(remoteConfig, 4)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.Run wf = session.cluster.Run wf
    override __.RunOnCurrentProcess wf = session.cluster.RunOnCurrentProcess wf
    override __.IsInMemoryFixture = false

[<TestFixture>]
type ``CloudValue Tests - Standalone Cluster - Storage Emulator``() = 
    inherit ``CloudValue Tests``(parallelismFactor = 5)
    let session = new ClusterSession(emulatorConfig, 4)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.Run wf = session.cluster.Run wf
    override __.RunOnCurrentProcess wf = session.cluster.RunOnCurrentProcess wf
    override __.IsSupportedLevel _ = true

[<TestFixture>]
type ``CloudValue Tests - Standalone Cluster - Remote Storage``() = 
    inherit ``CloudValue Tests``(parallelismFactor = 5)
    let session = new ClusterSession(remoteConfig, 4)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.Run wf = session.cluster.Run wf
    override __.RunOnCurrentProcess wf = session.cluster.RunOnCurrentProcess wf
    override __.IsSupportedLevel _ = true
