namespace MBrace.Azure.Tests.Runtime

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Tests
open MBrace.Runtime
open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Tests

#nowarn "445" // 'Reset'

[<AbstractClass; TestFixture>]
type ``Azure Cloud Tests`` (session : RuntimeSession) as self =
    inherit ``Cloud Tests`` (parallelismFactor = 20, delayFactor = 15000)
    
    let session = session 

    let run (wf : Cloud<'T>) = self.RunOnCloud wf

    member this.Session = session

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop()

    override __.RunOnCloud (workflow : Cloud<'T>) = 
        session.Runtime.RunOnCloudAsync (workflow)
        |> Async.Catch
        |> Async.RunSync

    override __.RunOnCloud (workflow : ICloudCancellationTokenSource -> #Cloud<'T>) = 
        async {
            let runtime = session.Runtime
            let cts = runtime.CreateCancellationTokenSource()
            try return! runtime.RunOnCloudAsync(workflow cts, cancellationToken = cts.Token) |> Async.Catch
            finally cts.Cancel()
        } |> Async.RunSync

    override __.RunOnCloudWithLogs(workflow : Cloud<unit>) =
        let task = session.Runtime.CreateCloudTask(workflow)
        do task.Result
        task.GetLogs () |> Array.map CloudLogEntry.Format

    override __.RunOnCurrentProcess(workflow : Cloud<'T>) = session.Runtime.RunOnCurrentProcess(workflow)

    override __.IsTargetWorkerSupported = true
    override __.IsSiftedWorkflowSupported = true
    override __.FsCheckMaxTests = 4
    override __.Repeats = 1
    override __.UsesSerialization = true

    [<Test>]
    member __.``Z4. Runtime : CloudValue`` () =
        let input = [|1 .. 400000|]
        let cloudValue = CloudValue.New(input, storageLevel = StorageLevel.Disk) |> session.Runtime.RunOnCloud
        cloudValue.Value |> shouldEqual input

    [<Test>]
    member __.``Z4. Runtime : Get worker count`` () =
        run (Cloud.GetWorkerCount()) |> Choice.shouldEqual (session.Runtime.Workers |> Seq.length)

    [<Test>]
    member __.``Z4. Runtime : Get current worker`` () =
        run Cloud.CurrentWorker |> Choice.shouldBe (fun _ -> true)

    [<Test>]
    member __.``Z4. Runtime : Get process id`` () =
        run (Cloud.GetProcessId()) |> Choice.shouldBe (fun _ -> true)

    [<Test>]
    member __.``Z4. Runtime : Get task id`` () =
        run (Cloud.GetJobId()) |> Choice.shouldBe (fun _ -> true)


type ``Compute - Storage Emulator`` () =
    inherit ``Azure Cloud Tests``(RuntimeSession(emulatorConfig, 0))
    
    [<TestFixtureSetUp>]
    member __.Init () = base.Init()
    [<TestFixtureTearDown>]
    member __.Fini () = base.Fini()

type ``Standalone - Storage Emulator`` () =
    inherit ``Azure Cloud Tests``(RuntimeSession(emulatorConfig, 4))

    [<TestFixtureSetUp>]
    member __.Init () = base.Init()
    [<TestFixtureTearDown>]
    member __.Fini () = base.Fini()


type ``Standalone`` () =
    inherit ``Azure Cloud Tests``(RuntimeSession(remoteConfig, 4))

    [<TestFixtureSetUp>]
    member __.Init () = base.Init()
    [<TestFixtureTearDown>]
    member __.Fini () = base.Fini()
