namespace MBrace.Azure.Runtime.Tests

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Flow.Tests
open MBrace.Azure
open MBrace.Azure.Client
open MBrace.Azure.Runtime

open NUnit.Framework

[<AbstractClass; TestFixture>]
type ``Azure Flow Tests`` (sbus, storage) as self =
    inherit ``CloudFlow tests`` ()

    let config = 
        { Configuration.Default with
            StorageConnectionString = storage
            ServiceBusConnectionString = sbus }

    let session = new RuntimeSession(config)

    let run (wf : Cloud<'T>) = self.Run wf

    member __.Configuration = config

    [<TestFixtureSetUp>]
    abstract Init : unit -> unit
    default __.Init () = session.Start()

    [<TestFixtureTearDown>]
    abstract Fini : unit -> unit
    default __.Fini () = session.Stop()

    override __.Run (workflow : Cloud<'T>) = 
        session.Runtime.Run(workflow)

    override __.RunLocally(workflow : Cloud<'T>) = session.Runtime.RunLocal(workflow)

    override __.FsCheckMaxNumberOfTests = 10
    override __.FsCheckMaxNumberOfIOBoundTests = 10

type ``Streams Compute - Storage Emulator`` () =
    inherit ``Azure Flow Tests``(Utils.selectEnv "azureservicebusconn", "UseDevelopmentStorage=true")
    
    [<TestFixtureSetUpAttribute>]
    override __.Init() =
        // TODO : Check if emulator is up?
        base.Init()    

type ``Streams Standalone - Storage Emulator`` () =
    inherit ``Azure Flow Tests``(Utils.selectEnv "azureservicebusconn", "UseDevelopmentStorage=true")

    [<TestFixtureSetUpAttribute>]
    override __.Init() =
        Runtime.LocalWorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.azureworker.exe"
        Runtime.SpawnLocal(base.Configuration, 4) 
        base.Init()
        
    [<TestFixtureTearDownAttribute>]
    override __.Fini() =
        System.Diagnostics.Process.GetProcessesByName "MBrace.Azure.Runtime.Standalone"
        |> Seq.iter (fun ps -> ps.Kill())
        base.Fini()


type ``Streams Standalone`` () =
    inherit ``Azure Flow Tests``(Utils.selectEnv "azureservicebusconn", Utils.selectEnv "azurestorageconn")
    
    [<TestFixtureSetUpAttribute>]
    override __.Init() =
        Runtime.LocalWorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.azureworker.exe"
        Runtime.SpawnLocal(base.Configuration, 4) 
        base.Init()
        
    [<TestFixtureTearDownAttribute>]
    override __.Fini() =
        System.Diagnostics.Process.GetProcessesByName "MBrace.Azure.Runtime.Standalone"
        |> Seq.iter (fun ps -> ps.Kill())
        base.Fini()