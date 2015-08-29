namespace MBrace.Azure.Tests.Runtime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Tests

open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Tests

open NUnit.Framework

[<AbstractClass; TestFixture>]
type ``Azure CloudFlow Tests`` (session : RuntimeSession) as self =
    inherit ``CloudFlow tests`` ()

    let session = session

    let run (wf : Cloud<'T>) = self.RunOnCloud wf

    member __.Session = session

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
    inherit ``Azure CloudFlow Tests``(RuntimeSession(emulatorConfig, 0))
    
    [<TestFixtureSetUp>]
    override __.Init () = base.Init()
    [<TestFixtureTearDown>]
    override __.Fini () = base.Fini()

type ``CloudFlow Standalone - Storage Emulator`` () =
    inherit ``Azure CloudFlow Tests``(RuntimeSession(emulatorConfig, 4))

    [<TestFixtureSetUp>]
    override __.Init () = base.Init()
    [<TestFixtureTearDown>]
    override __.Fini () = base.Fini()
        
type ``CloudFlow Standalone`` () =
    inherit ``Azure CloudFlow Tests``(RuntimeSession(remoteConfig, 0))
    
    [<TestFixtureSetUp>]
    override __.Init () = base.Init()
    [<TestFixtureTearDown>]
    override __.Fini () = base.Fini()