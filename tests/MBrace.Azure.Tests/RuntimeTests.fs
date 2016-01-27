namespace MBrace.Azure.Tests.Runtime

open System
open System.Threading

open NUnit.Framework

open MBrace.Core
open MBrace.Core.BuilderAsyncExtensions
open MBrace.Library.Protected
open MBrace.Core.Internals
open MBrace.Core.Tests
open MBrace.Runtime
open MBrace.Azure
open MBrace.Azure.Tests

#nowarn "444"

[<AbstractClass; TestFixture>]
type ``Azure Runtime Tests`` (config : Configuration, localWorkers : int) =

    let repeats =
#if DEBUG
        3
#else
        1
#endif

    let session = new ClusterSession(config, localWorkers)
    let run w = session.Cluster.Run(w)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop()

    [<Test>]
    member __.``1. Runtime : Get worker count`` () =
        run (Cloud.GetWorkerCount()) |> shouldEqual (session.Cluster.Workers |> Seq.length)

    [<Test>]
    member __.``1. Runtime : Get current worker`` () =
        run Cloud.CurrentWorker |> shouldBe (fun _ -> true)

    [<Test>]
    member __.``1. Runtime : Get task id`` () =
        run (Cloud.GetCloudProcessId()) |> shouldBe (fun _ -> true)

    [<Test>]
    member __.``1. Runtime : Get work item id`` () =
        run (Cloud.GetWorkItemId()) |> shouldBe (fun _ -> true)

    [<Test>]
    member __.``1. Runtime : Worker Log Observable`` () =
        let cluster = session.Cluster
        let worker = cluster.Workers.[0]
        let ra = new ResizeArray<SystemLogEntry>()
        use d = worker.SystemLogs.Subscribe ra.Add
        cluster.Run(cloud { return () }, target = worker)
        System.Threading.Thread.Sleep 2000
        ra.Count |> shouldBe (fun i -> i > 0)

    [<Test>]
    member __.``1. Runtime : Additional resources`` () =
        let cluster = session.Cluster
        let res = (42, "forty-two")
        cluster.Run(Cloud.GetResource<int * string>(), additionalResources = resource { yield res })
        |> shouldEqual res

    [<Test>]
    member __.``1. Runtime : Cluster Log Observable`` () =
        let cluster = session.Cluster
        let ra = new ResizeArray<SystemLogEntry>()
        use d = cluster.SystemLogs.Subscribe ra.Add
        cluster.Run(Cloud.ParallelEverywhere(cloud { return 42 }) |> Cloud.Ignore)
        System.Threading.Thread.Sleep 2000
        ra.Count |> shouldBe (fun i -> i >= cluster.Workers.Length)

    [<Test>]
    member __.``1. Runtime : CloudProcess Log Observable`` () =
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
        let job = session.Cluster.CreateProcess(workflow)
        use d = job.Logs.Subscribe(fun e -> ra.Add(e))
        do job.Result
        ra |> Seq.filter (fun e -> e.Message.Contains "Work item") |> Seq.length |> shouldEqual 2000

    [<Test>]
    member __.``2. Fault Tolerance : Custom fault policy`` () =
        repeat repeats (fun () ->
            let cluster = session.Cluster
            let f = cluster.Store.CloudAtom.Create(false)
            let t = cluster.CreateProcess(cloud {
                do! f.ForceAsync true
                do! Cloud.Sleep 20000
            }, faultPolicy = FaultPolicy.NoRetry)
            while not f.Value do Thread.Sleep 1000
            session.Chaos()
            Choice.protect (fun () -> t.Result) |> Choice.shouldFailwith<_, FaultException>)

    [<Test>]
    member __.``2. Fault Tolerance : Custom fault policy nested`` () =
        repeat repeats (fun () ->
            let cluster = session.Cluster
            let f = cluster.Store.CloudAtom.Create(false)
            let computation () = cloud {
                let child () = cloud { 
                    do! f.ForceAsync true 
                    do! Cloud.Sleep 20000
                }

                return! Cloud.WithFaultPolicy FaultPolicy.NoRetry (child () <||> child ())
            }

            let t = cluster.CreateProcess(computation (), faultPolicy = FaultPolicy.InfiniteRetries())
            while not f.Value do Thread.Sleep 1000
            session.Chaos()
            Choice.protect (fun () -> t.Result) |> Choice.shouldFailwith<_, FaultException>)

    [<Test>]
    member __.``2. Fault Tolerance : targeted workers`` () =
        repeat repeats (fun () ->
            let cluster = session.Cluster
            let f = cluster.Store.CloudAtom.Create(false)
            let wf () = cloud {
                let! current = Cloud.CurrentWorker
                // targeted work items should fail regardless of fault policy
                return! 
                    Cloud.CreateProcess(cloud { 
                        do! f.ForceAsync true 
                        do! Cloud.Sleep 20000 }, target = current, faultPolicy = FaultPolicy.InfiniteRetries())
            }

            let t = cluster.Run (wf ())
            while not f.Value do Thread.Sleep 1000
            session.Chaos()
            Choice.protect(fun () -> t.Result) |> Choice.shouldFailwith<_, FaultException>)

    [<Test>]
    member __.``2. Fault Tolerance : fault data`` () =
        session.Cluster.Run(Cloud.TryGetFaultData()) |> shouldBe Option.isNone

        repeat repeats (fun () ->
            let cluster = session.Cluster
            let f = cluster.Store.CloudAtom.Create(false)
            let t = 
                cluster.CreateProcess(
                    cloud {
                        do! f.ForceAsync true
                        do! Cloud.Sleep 20000
                        return! Cloud.TryGetFaultData()
                    }, faultPolicy = FaultPolicy.WithMaxRetries 1)

            while not f.Value do Thread.Sleep 1000
            session.Chaos()
            t.Result |> shouldBe (function Some { NumberOfFaults = 1 } -> true | _ -> false))

    [<Test>]
    member __.``2. Fault Tolerance : protected parallel workflows`` () =
        repeat repeats (fun () ->
            let cluster = session.Cluster
            let localWorkers = cluster.Workers.Length
            let f = cluster.Store.CloudAtom.Create 0
            let task i = cloud {
                let! _ = CloudAtom.Increment f
                do! Cloud.Sleep 20000
                return i
            }

            let cloudProcess = 
                cluster.CreateProcess(
                    [for i in 1 .. localWorkers -> task i]
                    |> Cloud.ProtectedParallel, faultPolicy = FaultPolicy.WithMaxRetries 1)

            while f.Value < localWorkers do Thread.Sleep 1000
            session.Chaos()
            cloudProcess.Result 
            |> Array.forall (function FaultException _ -> true | _ -> false)
            |> shouldEqual true)

    [<Test>]
    member __.``2. Fault Tolerance : map/reduce`` () =
        repeat 2 (fun () ->
            let cluster = session.Cluster
            let f = cluster.Store.CloudAtom.Create(false)
            let t = cluster.CreateProcess(cloud {
                do! f.ForceAsync true
                return! WordCount.run 20 WordCount.mapReduceRec
            }, faultPolicy = FaultPolicy.InfiniteRetries())
            while not f.Value do Thread.Sleep 1000
            do Thread.Sleep 1000
            session.Chaos()
            t.Result |> shouldEqual 100)


    [<Test>]
    member __.``2. Fault Tolerance : faulted process status `` () =
        repeat repeats (fun () ->
            let runtime = session.Cluster
            let f = runtime.Store.CloudAtom.Create(false)
            let wf () = cloud {
                let worker () = cloud {
                    do! f.ForceAsync true 
                    do! Cloud.Sleep 60000
                }

                return! Cloud.ParallelEverywhere(worker())
            }

            let t = runtime.CreateProcess (wf (), faultPolicy = FaultPolicy.NoRetry)
            while not f.Value do Thread.Sleep 1000
            session.Chaos()
            Choice.protect(fun () -> t.Result) |> Choice.shouldFailwith<_, FaultException>
            t.Status |> shouldEqual CloudProcessStatus.Faulted)

[<Category("Storage Emulator")>]
type ``Runtime Tests - Standalone Cluster - Storage Emulator`` () =
    inherit ``Azure Runtime Tests``(mkEmulatorConfig (), 4)

[<Category("Standalone Cluster")>]
type ``Runtime Tests - Standalone Cluster - Remote Storage`` () =
    inherit ``Azure Runtime Tests``(mkRemoteConfig (), 4)