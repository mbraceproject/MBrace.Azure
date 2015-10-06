namespace MBrace.Azure.Tests.Runtime

open System
open System.Threading

open NUnit.Framework

open MBrace.Core
open MBrace.Library.Protected
open MBrace.Core.Internals
open MBrace.Core.Tests
open MBrace.Runtime
open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Tests

#nowarn "444"

[<AbstractClass; TestFixture>]
type ``Azure Cloud Tests`` (session : ClusterSession) as self =
    inherit ``Cloud Tests`` (parallelismFactor = 20, delayFactor = 15000)
    
    let session = session 

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


[<AbstractClass; TestFixture>]
type ``Azure Specialized Cloud Tests`` (session : ClusterSession) =

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
        let job = session.Cluster.Submit(workflow)
        use d = job.Logs.Subscribe(fun e -> ra.Add(e))
        do job.Result
        ra |> Seq.filter (fun e -> e.Message.Contains "Work item") |> Seq.length |> shouldEqual 2000


    [<Test>]
    member __.``2. Fault Tolerance : map/reduce`` () =
        repeat 3 (fun () ->
            let cluster = session.Cluster
            let f = cluster.Store.Atom.Create(false)
            let t = cluster.Submit(cloud {
                do! f.Force true
                return! WordCount.run 20 WordCount.mapReduceRec
            }, faultPolicy = FaultPolicy.InfiniteRetries())
            while not f.Value do Thread.Sleep 1000
            do Thread.Sleep 1000
            session.Chaos()
            t.Result |> shouldEqual 100)


    [<Test>]
    member __.``2. Fault Tolerance : Custom fault policy 1`` () =
        repeat 5 (fun () ->
            let cluster = session.Cluster
            let f = cluster.Store.Atom.Create(false)
            let t = cluster.Submit(cloud {
                do! f.Force true
                do! Cloud.Sleep 20000
            }, faultPolicy = FaultPolicy.NoRetry)
            while not f.Value do Thread.Sleep 1000
            session.Chaos()
            Choice.protect (fun () -> t.Result) |> Choice.shouldFailwith<_, FaultException>)

    [<Test>]
    member __.``2. Fault Tolerance : Custom fault policy 2`` () =
        repeat 5 (fun () ->
            let cluster = session.Cluster
            let f = cluster.Store.Atom.Create(false)
            let t = cluster.Submit(cloud {
                return! 
                    Cloud.WithFaultPolicy FaultPolicy.NoRetry
                        (cloud { 
                            do! f.Force(true) 
                            return! Cloud.Sleep 20000 <||> Cloud.Sleep 20000
                        })
            })
            while not f.Value do Thread.Sleep 1000
            session.Chaos()
            Choice.protect (fun () -> t.Result) |> Choice.shouldFailwith<_, FaultException>)

    [<Test>]
    member __.``2. Fault Tolerance : targeted workers`` () =
        repeat 5(fun () ->
            let cluster = session.Cluster
            let f = cluster.Store.Atom.Create(false)
            let wf () = cloud {
                let! current = Cloud.CurrentWorker
                // targeted work items should fail regardless of fault policy
                return! 
                    Cloud.StartAsCloudProcess(cloud { 
                        do! f.Force true 
                        do! Cloud.Sleep 20000 }, target = current, faultPolicy = FaultPolicy.InfiniteRetries())
            }

            let t = cluster.Run (wf ())
            while not f.Value do Thread.Sleep 1000
            session.Chaos()
            Choice.protect(fun () -> t.Result) |> Choice.shouldFailwith<_, FaultException>)

    [<Test>]
    member __.``2. Fault Tolerance : fault data`` () =
        session.Cluster.Run(Cloud.TryGetFaultData()) |> shouldBe Option.isNone

        repeat 5 (fun () ->
            let cluster = session.Cluster
            let f = cluster.Store.Atom.Create(false)
            let t = 
                cluster.Submit(
                    cloud {
                        do! f.Force true
                        do! Cloud.Sleep 5000
                        return! Cloud.TryGetFaultData()
                    })

            while not f.Value do Thread.Sleep 1000
            session.Chaos()
            t.Result |> shouldBe (function Some { NumberOfFaults = 1 } -> true | _ -> false))

    [<Test>]
    member __.``2. Fault Tolerance : protected parallel workflows`` () =
        repeat 5 (fun () ->
            let cluster = session.Cluster
            let f = cluster.Store.Atom.Create(false)
            let task i = cloud {
                do! f.Force true
                do! Cloud.Sleep 5000
                return i
            }

            let cloudProcess =
                [for i in 1 .. 10 -> task i]
                |> Cloud.ProtectedParallel
                |> cluster.Submit

            while not f.Value do Thread.Sleep 1000
            session.Chaos()
            cloudProcess.Result 
            |> Array.forall (function FaultException _ -> true | _ -> false)
            |> shouldEqual true)



type ``Cloud Tests - Compute Emulator - Storage Emulator`` () =
    inherit ``Azure Cloud Tests``(ClusterSession(emulatorConfig, 0))

type ``Cloud Tests - Standalone Cluster - Storage Emulator`` () =
    inherit ``Azure Cloud Tests``(ClusterSession(emulatorConfig, 4))

type ``Cloud Tests - Standalone Cluster - Remote Storage`` () =
    inherit ``Azure Cloud Tests``(ClusterSession(remoteConfig, 4))

type ``Specialized Cloud Tests - Compute Emulator - Storage Emulator`` () =
    inherit ``Azure Specialized Cloud Tests``(ClusterSession(emulatorConfig, 0))

type ``Specialized Cloud Tests - Standalone Cluster - Storage Emulator`` () =
    inherit ``Azure Specialized Cloud Tests``(ClusterSession(emulatorConfig, 4))

type ``Specialized Cloud Tests - Standalone Cluster - Remote Storage`` () =
    inherit ``Azure Specialized Cloud Tests``(ClusterSession(remoteConfig, 4))