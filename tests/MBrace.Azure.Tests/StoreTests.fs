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
type ``Azure BlobStore Tests``(config : Configuration, localWorkers : int) = 
    inherit ``CloudFileStore Tests``(parallelismFactor = 10)
    let session = new ClusterSession(config, localWorkers)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()

    [<Test>]
    member __.``Should correctly support nested subdirectories`` () =
        cloud {
            let! fs = CloudStore.FileSystem
            let dirName = fs.Path.GetRandomDirectoryName()
            use dir = fs.Directory.Create dirName
            let subDir = fs.Path.Combine(dirName, "foo", "bar")
            let file = fs.Path.Combine(subDir, "test.txt")
            let cf = fs.File.WriteAllText(file, "lorem ipsum dolor sit amet")
            fs.Directory.Enumerate(dirName).Length |> shouldEqual 1
            fs.File.Enumerate(subDir).Length |> shouldEqual 1
        } |> session.Cluster.Run
    
    override __.FileStore = session.Cluster.GetResource<ICloudFileStore>()
    override __.Serializer = session.Cluster.GetResource<ISerializer>()
    override __.IsCaseSensitive = false
    override __.Run(workflow : Cloud<'T>) = session.Cluster.Run workflow
    override __.RunLocally(workflow : Cloud<'T>) = session.Cluster.RunLocally workflow

[<TestFixture; Category("Standalone Cluster")>]
type ``BlobStore Tests - Standalone Cluster - Remote Storage``() =
    inherit ``Azure BlobStore Tests``(mkRemoteConfig (), 4)

[<TestFixture; Category("Storage Emulator")>]
type ``BlobStore Tests - Standalone Cluster - Storage Emulator``() = 
    inherit ``Azure BlobStore Tests``(mkEmulatorConfig (), 4)

[<TestFixture; Category("Remote Cluster")>]
type ``BlobStore Tests - Remote Cluster - Remote Storage``() = 
    inherit ``Azure BlobStore Tests``(mkRemoteConfig (), 0)


// CloudAtom tests


[<AbstractClass; TestFixture>]
type ``Azure CloudAtom Tests``(config : Configuration, localWorkers : int) = 
    inherit ``CloudAtom Tests``(parallelismFactor = 5)
    let session = new ClusterSession(config, localWorkers)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.IsSupportedNamedLookup = true
    override __.Run wf = session.Cluster.Run wf
    override __.RunLocally wf = session.Cluster.RunLocally wf
    override __.Repeats = 1

[<TestFixture; Category("Standalone Cluster")>]
type ``CloudAtom Tests - Standalone Cluster - Remote Storage``() = 
    inherit ``Azure CloudAtom Tests``(mkRemoteConfig (), 4)

[<TestFixture; Category("Storage Emulator")>]
type ``CloudAtom Tests - Standalone Cluster - Storage Emulator``() = 
    inherit ``Azure CloudAtom Tests``(mkEmulatorConfig (), 4)
    override __.Repeats = 3

[<TestFixture; Category("Remote Cluster")>]
type ``CloudAtom Tests - Remote Cluster - Remote Storage``() = 
    inherit ``Azure CloudAtom Tests``(mkRemoteConfig (), 0)


// CloudQueue Tests


[<AbstractClass; TestFixture>]
type ``Azure CloudQueue Tests``(config : Configuration, localWorkers : int) = 
    inherit ``CloudQueue Tests``(parallelismFactor = 10)
    let session = new ClusterSession(config, localWorkers)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.Run wf = session.Cluster.Run wf
    override __.RunLocally wf = session.Cluster.RunLocally wf
    override __.IsSupportedNamedLookup = true

[<TestFixture; Category("Standalone Cluster")>]
type ``CloudQueue Tests - Standalone Cluster - Remote Storage``() = 
    inherit ``Azure CloudQueue Tests``(mkRemoteConfig (), 4)

[<TestFixture; Category("Remote Cluster")>]
type ``CloudQueue Tests - Remote Cluster - Remote Storage``() = 
    inherit ``Azure CloudQueue Tests``(mkRemoteConfig (), 0)


// CloudDictionary tests


[<AbstractClass; TestFixture>]
type ``Azure CloudDictionary Tests``(config : Configuration, localWorkers : int) = 
    inherit ``CloudDictionary Tests``(parallelismFactor = 5)
    let session = new ClusterSession(config, localWorkers)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.Run wf = session.Cluster.Run wf
    override __.RunLocally wf = session.Cluster.RunLocally wf
    override __.IsInMemoryFixture = false
    override __.IsSupportedNamedLookup = true

[<TestFixture; Category("Storage Emulator")>]
type ``CloudDictionary Tests - Standalone Cluster - Storage Emulator``() = 
    inherit ``Azure CloudDictionary Tests``(mkEmulatorConfig (), 4)

[<TestFixture; Category("Standalone Cluster")>]
type ``CloudDictionary Tests - Standalone Cluster - Remote Storage``() = 
    inherit ``Azure CloudDictionary Tests``(mkRemoteConfig (), 4)

[<TestFixture; Category("Remote Cluster")>]
type ``CloudDictionary Tests - Remote Cluster - Remote Storage``() = 
    inherit ``Azure CloudDictionary Tests``(mkRemoteConfig (), 0)


// CloudValue tests


[<AbstractClass; TestFixture>]
type ``Azure CloudValue Tests``(config : Configuration, localWorkers : int) = 
    inherit ``CloudValue Tests``(parallelismFactor = 5)
    let session = new ClusterSession(config, localWorkers)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.Run wf = session.Cluster.Run wf
    override __.RunLocally wf = session.Cluster.RunLocally wf
    override __.IsSupportedLevel _ = true

[<TestFixture; Category("Storage Emulator")>]
type ``CloudValue Tests - Standalone Cluster - Storage Emulator``() = 
    inherit ``Azure CloudValue Tests``(mkEmulatorConfig (), 4)

[<TestFixture; Category("Standalone Cluster")>]
type ``CloudValue Tests - Standalone Cluster - Remote Storage``() = 
    inherit ``Azure CloudValue Tests``(mkRemoteConfig (), 4)

[<TestFixture; Category("Remote Cluster")>]
type ``CloudValue Tests - Remote Cluster - Remote Storage``() = 
    inherit ``Azure CloudValue Tests``(mkRemoteConfig (), 0)