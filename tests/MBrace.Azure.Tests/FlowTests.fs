namespace MBrace.Azure.Tests.Runtime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Tests

open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Tests

open NUnit.Framework

[<AbstractClass; TestFixture>]
type ``Azure CloudFlow Tests`` (sbus, storage) as self =
    inherit ``CloudFlow tests`` ()

    let config = new Configuration(storage, sbus)

    let session = new RuntimeSession(config)

    let run (wf : Cloud<'T>) = self.RunOnCloud wf

    member __.Configuration = config

    [<TestFixtureSetUp>]
    abstract Init : unit -> unit
    default __.Init () = session.Start()

    [<TestFixtureTearDown>]
    abstract Fini : unit -> unit
    default __.Fini () = session.Stop()

    override __.IsSupportedStorageLevel _ = true

    override __.RunOnCloud (workflow : Cloud<'T>) = 
        session.Runtime.RunOnCloud(workflow)

    override __.RunOnCurrentProcess(workflow : Cloud<'T>) = session.Runtime.RunOnCurrentProcess(workflow)

    override __.FsCheckMaxNumberOfTests = 3
    override __.FsCheckMaxNumberOfIOBoundTests = 3

type ``CloudFlow Compute - Storage Emulator`` () =
    inherit ``Azure CloudFlow Tests``(Utils.selectEnv "azureservicebusconn", "UseDevelopmentStorage=true")
    
    [<TestFixtureSetUpAttribute>]
    override __.Init() =
        // TODO : Check if emulator is up?
        base.Init()    

type ``CloudFlow Standalone - Storage Emulator`` () =
    inherit ``Azure CloudFlow Tests``(Utils.selectEnv "azureservicebusconn", "UseDevelopmentStorage=true")

    [<TestFixtureSetUpAttribute>]
    override __.Init() =
        MBraceCluster.LocalWorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.azureworker.exe"
        MBraceCluster.SpawnOnCurrentMachine(base.Configuration, 4, 16) 
        base.Init()
        
    [<TestFixtureTearDownAttribute>]
    override __.Fini() =
        System.Diagnostics.Process.GetProcessesByName "MBrace.Azure.Runtime.Standalone"
        |> Seq.iter (fun ps -> ps.Kill())
        base.Fini()


type ``CloudFlow Standalone`` () =
    inherit ``Azure CloudFlow Tests``(Utils.selectEnv "azureservicebusconn", Utils.selectEnv "azurestorageconn")
    
    [<TestFixtureSetUpAttribute>]
    override __.Init() =
        MBraceCluster.LocalWorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.azureworker.exe"
        MBraceCluster.SpawnOnCurrentMachine(base.Configuration, 4, 16) 
        base.Init()
        
    [<TestFixtureTearDownAttribute>]
    override __.Fini() =
        System.Diagnostics.Process.GetProcessesByName "MBrace.Azure.Runtime.Standalone"
        |> Seq.iter (fun ps -> ps.Kill())
        base.Fini()