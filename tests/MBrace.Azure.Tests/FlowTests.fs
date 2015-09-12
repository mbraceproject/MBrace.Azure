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

    let run (wf : Cloud<'T>) = self.Run wf

    member __.Session = session

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop()

    override __.IsSupportedStorageLevel _ = true

    override __.Run (workflow : Cloud<'T>) = 
        session.Runtime.Run(workflow)

    override __.RunOnCurrentProcess(workflow : Cloud<'T>) = 
        session.Runtime.RunOnCurrentProcess(workflow)

    override __.FsCheckMaxNumberOfTests = 3
    override __.FsCheckMaxNumberOfIOBoundTests = 3

type ``CloudFlow Tests - Compute Emulator`` () =
    inherit ``Azure CloudFlow Tests``(RuntimeSession(emulatorConfig, 0))

type ``CloudFlow Tests - Storage Emulator`` () =
    inherit ``Azure CloudFlow Tests``(RuntimeSession(emulatorConfig, 4))
        
type ``CloudFlow Tests - Standalone`` () =
    inherit ``Azure CloudFlow Tests``(RuntimeSession(remoteConfig, 4))