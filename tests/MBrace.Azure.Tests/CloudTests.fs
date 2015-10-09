namespace MBrace.Azure.Tests.Runtime

open System

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Tests
open MBrace.Azure
open MBrace.Azure.Tests

[<AbstractClass; TestFixture>]
type ``Azure Cloud Tests`` (session : LocalClusterSession) as self =
    inherit ``Cloud Tests`` (parallelismFactor = 20, delayFactor = 15000)

    let run (wf : Cloud<'T>) = self.Run wf

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop()

    override __.Run (workflow : Cloud<'T>) = 
        session.Cluster.RunAsync (workflow)
        |> Async.Catch
        |> Async.RunSync

    override __.Run (workflow : ICloudCancellationTokenSource -> #Cloud<'T>) = 
        async {
            let cluster = session.Cluster
            let cts = cluster.CreateCancellationTokenSource()
            try return! cluster.RunAsync(workflow cts, cancellationToken = cts.Token) |> Async.Catch
            finally cts.Cancel()
        } |> Async.RunSync

    override __.RunWithLogs(workflow : Cloud<unit>) =
        let cloudProcess = session.Cluster.Submit workflow
        do cloudProcess.Result
        cloudProcess.GetLogs () |> Array.map CloudLogEntry.Format

    override __.RunOnCurrentProcess(workflow : Cloud<'T>) = session.Cluster.RunOnCurrentProcess(workflow)

    override __.IsTargetWorkerSupported = true
    override __.IsSiftedWorkflowSupported = true
    override __.FsCheckMaxTests = 4
    override __.Repeats = 1
    override __.UsesSerialization = true

type ``Cloud Tests - Compute Emulator - Storage Emulator`` () =
    inherit ``Azure Cloud Tests``(LocalClusterSession(emulatorConfig, 0))

type ``Cloud Tests - Standalone Cluster - Storage Emulator`` () =
    inherit ``Azure Cloud Tests``(LocalClusterSession(emulatorConfig, 4))

type ``Cloud Tests - Standalone Cluster - Remote Storage`` () =
    inherit ``Azure Cloud Tests``(LocalClusterSession(remoteConfig, 4))

type ``Cloud Tests - Remote Cluster - Remote Storage`` () =
    inherit ``Azure Cloud Tests``(LocalClusterSession(remoteConfig, 0))