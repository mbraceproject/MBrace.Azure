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
type ``BlobStore Tests - Remote``() = 
    inherit ``CloudFileStore Tests``(parallelismFactor = 10)
    let session = new RuntimeSession(remoteConfig, 4)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.FileStore = session.Runtime.GetResource<ICloudFileStore>()
    override __.Serializer = session.Runtime.GetResource<ISerializer>()
    override __.RunOnCloud(workflow : Cloud<'T>) = session.Runtime.RunOnCloud workflow
    override __.RunOnCurrentProcess(workflow : Cloud<'T>) = session.Runtime.RunOnCurrentProcess workflow

[<TestFixture>]
type ``BlobStore Tests - Storage Emulator``() = 
    inherit ``CloudFileStore Tests``(parallelismFactor = 10)
    let session = new RuntimeSession(emulatorConfig, 4)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.FileStore = session.Runtime.GetResource<ICloudFileStore>()
    override __.Serializer = session.Runtime.GetResource<ISerializer>()
    override __.RunOnCloud(workflow : Cloud<'T>) = session.Runtime.RunOnCloud workflow
    override __.RunOnCurrentProcess(workflow : Cloud<'T>) = session.Runtime.RunOnCurrentProcess workflow

[<TestFixture>]
type ``Atom Tests - Remote``() = 
    inherit ``CloudAtom Tests``(parallelismFactor = 5)
    let session = new RuntimeSession(remoteConfig, 4)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.RunOnCloud wf = session.Runtime.RunOnCloud wf
    override __.RunOnCurrentProcess wf = session.Runtime.RunOnCurrentProcess wf
    override __.Repeats = 1

[<TestFixture>]
type ``Atom Tests - Storage Emulator``() = 
    inherit ``CloudAtom Tests``(parallelismFactor = 5)
    let session = new RuntimeSession(emulatorConfig, 4)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.RunOnCloud wf = session.Runtime.RunOnCloud wf
    override __.RunOnCurrentProcess wf = session.Runtime.RunOnCurrentProcess wf
    override __.Repeats = 3

[<TestFixture>]
type ``Queue Tests - Remote``() = 
    inherit ``CloudQueue Tests``(parallelismFactor = 10)
    let session = new RuntimeSession(remoteConfig, 4)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.RunOnCloud wf = session.Runtime.RunOnCloud wf
    override __.RunOnCurrentProcess wf = session.Runtime.RunOnCurrentProcess wf

[<TestFixture>]
type ``Dictionary Tests - Storage Emulator``() = 
    inherit ``CloudDictionary Tests``(parallelismFactor = 5)
    let session = new RuntimeSession(emulatorConfig, 4)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.RunOnCloud wf = session.Runtime.RunOnCloud wf
    override __.RunOnCurrentProcess wf = session.Runtime.RunOnCurrentProcess wf
    override __.IsInMemoryFixture = false

[<TestFixture>]
type ``Dictionary Tests - Remote``() = 
    inherit ``CloudDictionary Tests``(parallelismFactor = 5)
    let session = new RuntimeSession(remoteConfig, 4)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.RunOnCloud wf = session.Runtime.RunOnCloud wf
    override __.RunOnCurrentProcess wf = session.Runtime.RunOnCurrentProcess wf
    override __.IsInMemoryFixture = false

[<TestFixture>]
type ``CloudValue Tests - Storage Emulator``() = 
    inherit ``CloudValue Tests``(parallelismFactor = 5)
    let session = new RuntimeSession(emulatorConfig, 4)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.RunOnCloud wf = session.Runtime.RunOnCloud wf
    override __.RunOnCurrentProcess wf = session.Runtime.RunOnCurrentProcess wf
    override __.IsSupportedLevel _ = true

[<TestFixture>]
type ``CloudValue Tests - Remote``() = 
    inherit ``CloudValue Tests``(parallelismFactor = 5)
    let session = new RuntimeSession(remoteConfig, 4)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.RunOnCloud wf = session.Runtime.RunOnCloud wf
    override __.RunOnCurrentProcess wf = session.Runtime.RunOnCurrentProcess wf
    override __.IsSupportedLevel _ = true
