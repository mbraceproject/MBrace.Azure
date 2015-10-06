namespace MBrace.Azure.Tests.Runtime

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Tests
open MBrace.Runtime
open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Tests

#nowarn "444"
#nowarn "445" // 'Reset'

[<AbstractClass; TestFixture>]
type ``Azure Cloud Tests`` (session : ClusterSession) as self =
    inherit ``Cloud Tests`` (parallelismFactor = 20, delayFactor = 15000)
    
    let session = session 

    let run (wf : Cloud<'T>) = self.Run wf

    member this.Session = session

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop()

    override __.Run (workflow : Cloud<'T>) = 
        session.cluster.RunAsync (workflow)
        |> Async.Catch
        |> Async.RunSync

    override __.Run (workflow : ICloudCancellationTokenSource -> #Cloud<'T>) = 
        async {
            let runtime = session.cluster
            let cts = runtime.CreateCancellationTokenSource()
            try return! runtime.RunAsync(workflow cts, cancellationToken = cts.Token) |> Async.Catch
            finally cts.Cancel()
        } |> Async.RunSync

    override __.RunWithLogs(workflow : Cloud<unit>) =
        let cloudProcess = session.cluster.Submit workflow
        do cloudProcess.Result
        cloudProcess.GetLogs () |> Array.map CloudLogEntry.Format

    override __.RunOnCurrentProcess(workflow : Cloud<'T>) = session.cluster.RunOnCurrentProcess(workflow)

    override __.IsTargetWorkerSupported = true
    override __.IsSiftedWorkflowSupported = true
    override __.FsCheckMaxTests = 4
    override __.Repeats = 1
    override __.UsesSerialization = true

    [<Test>]
    member __.``Runtime : Get worker count`` () =
        run (Cloud.GetWorkerCount()) |> Choice.shouldEqual (session.cluster.Workers |> Seq.length)

    [<Test>]
    member __.``Runtime : Get current worker`` () =
        run Cloud.CurrentWorker |> Choice.shouldBe (fun _ -> true)

    [<Test>]
    member __.``Runtime : Get task id`` () =
        run (Cloud.GetCloudProcessId()) |> Choice.shouldBe (fun _ -> true)

    [<Test>]
    member __.``Runtime : Get work item id`` () =
        run (Cloud.GetWorkItemId()) |> Choice.shouldBe (fun _ -> true)

    [<Test>]
    member __.``Runtime : Worker Log Observable`` () =
        let cluster = session.cluster
        let worker = cluster.Workers.[0]
        let ra = new ResizeArray<SystemLogEntry>()
        use d = worker.SystemLogs.Subscribe ra.Add
        cluster.Run(cloud { return () }, target = worker)
        System.Threading.Thread.Sleep 2000
        ra.Count |> shouldBe (fun i -> i > 0)

    [<Test>]
    member __.``Runtime : additional resources`` () =
        let cluster = session.cluster
        let res = (42, "forty-two")
        cluster.Run(Cloud.GetResource<int * string>(), additionalResources = resource { yield res })
        |> shouldEqual res

    [<Test>]
    member __.``Runtime : Cluster Log Observable`` () =
        let cluster = session.cluster
        let ra = new ResizeArray<SystemLogEntry>()
        use d = cluster.SystemLogs.Subscribe ra.Add
        cluster.Run(Cloud.ParallelEverywhere(cloud { return 42 }) |> Cloud.Ignore)
        System.Threading.Thread.Sleep 2000
        ra.Count |> shouldBe (fun i -> i >= cluster.Workers.Length)

    [<Test>]
    member __.``Runtime : CloudProcess Log Observable`` () =
        let workflow = cloud {
            let workItem i = local {
                for j in 1 .. 100 do
                    do! Cloud.Logf "Work item %d, iteration %d" i j
            }

            do! Cloud.Sleep 5000
            do! Cloud.Parallel [for i in 1 .. 20 -> workItem i] |> Cloud.Ignore
            do! Cloud.Sleep 2000
        }

        let ra = new ResizeArray<CloudLogEntry>()
        let job = session.cluster.Submit(workflow)
        use d = job.Logs.Subscribe(fun e -> ra.Add(e))
        do job.Result
        ra |> Seq.filter (fun e -> e.Message.Contains "Work item") |> Seq.length |> shouldEqual 2000


type ``Cloud Tests - Compute Emulator - Storage Emulator`` () =
    inherit ``Azure Cloud Tests``(ClusterSession(emulatorConfig, 0))

type ``Cloud Tests - Standalone Cluster - Storage Emulator`` () =
    inherit ``Azure Cloud Tests``(ClusterSession(emulatorConfig, 4))

type ``Cloud Tests - Standalone Cluster - Remote Storage`` () =
    inherit ``Azure Cloud Tests``(ClusterSession(remoteConfig, 4))