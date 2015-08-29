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
    open MBrace.Azure
    
    
[<TestFixture>]
type ``Remote - BlobStore Tests``() = 
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
type ``Storage Emulator - BlobStore Tests``() = 
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
type ``Remote - Atom Tests``() = 
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
type ``Storage Emulator - Atom Tests``() = 
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
type ``Remote - Queue Tests``() = 
    inherit ``CloudQueue Tests``(parallelismFactor = 10)
    let session = new RuntimeSession(remoteConfig, 4)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.RunOnCloud wf = session.Runtime.RunOnCloud wf
    override __.RunOnCurrentProcess wf = session.Runtime.RunOnCurrentProcess wf

[<TestFixture>]
type ``Storage Emulator - Dictionary Tests``() = 
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
type ``Remote - Dictionary Tests``() = 
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
type ``Storage Emulator - CloudValue Tests``() = 
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
type ``Remote - CloudValue Tests``() = 
    inherit ``CloudValue Tests``(parallelismFactor = 5)
    let session = new RuntimeSession(remoteConfig, 4)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.RunOnCloud wf = session.Runtime.RunOnCloud wf
    override __.RunOnCurrentProcess wf = session.Runtime.RunOnCurrentProcess wf
    override __.IsSupportedLevel _ = true
