namespace MBrace.Azure.Tests.Runtime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Tests

open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Tests

open NUnit.Framework

[<AbstractClass; TestFixture>]
type ``Azure CloudFlow Tests`` (session : ClusterSession) as self =
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
        session.Cluster.Run(workflow)

    override __.RunOnCurrentProcess(workflow : Cloud<'T>) = 
        session.Cluster.RunOnCurrentProcess(workflow)

    override __.FsCheckMaxNumberOfTests = 3
    override __.FsCheckMaxNumberOfIOBoundTests = 3

type ``CloudFlow Tests - Compute Emulator - Remote Storage`` () =
    inherit ``Azure CloudFlow Tests``(ClusterSession(emulatorConfig, 0))

type ``CloudFlow Tests - Standalone Cluster - Storage Emulator`` () =
    inherit ``Azure CloudFlow Tests``(ClusterSession(emulatorConfig, 4))
        
type ``CloudFlow Tests - Standalone Cluster - Remote Storage`` () =
    inherit ``Azure CloudFlow Tests``(ClusterSession(remoteConfig, 4))