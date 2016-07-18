namespace MBrace.Azure.Tests.Runtime

open System
open System.Threading

open NUnit.Framework
open Swensen.Unquote.Assertions

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
        test <@ run (Cloud.GetWorkerCount()) = session.Cluster.Workers.Length @>

    [<Test>]
    member __.``1. Runtime : Get current worker`` () =
        run Cloud.CurrentWorker |> ignore

    [<Test>]
    member __.``1. Runtime : Get task id`` () =
        run (Cloud.GetCloudProcessId()) |> ignore

    [<Test>]
    member __.``1. Runtime : Get work item id`` () =
        run (Cloud.GetWorkItemId()) |> ignore

    [<Test>]
    member __.``1. Runtime : Worker Log Observable`` () =
        let cluster = session.Cluster
        let worker = cluster.Workers.[0]
        let ra = new ResizeArray<SystemLogEntry>()
        use d = worker.SystemLogs.Subscribe ra.Add
        cluster.Run(cloud { return () }, target = worker)
        System.Threading.Thread.Sleep 2000
        test <@ ra.Count > 0 @>

    [<Test>]
    member __.``1. Runtime : Additional resources`` () =
        let cluster = session.Cluster
        let resources = (42, "forty-two")
        let result = cluster.Run(Cloud.GetResource<int * string>(), additionalResources = resource { yield resources })
        test <@ result = resources @>

    [<Test>]
    member __.``1. Runtime : Cluster Log Observable`` () =
        let cluster = session.Cluster
        let ra = new ResizeArray<SystemLogEntry>()
        use d = cluster.SystemLogs.Subscribe ra.Add
        cluster.Run(Cloud.ParallelEverywhere(cloud { return 42 }) |> Cloud.Ignore)
        System.Threading.Thread.Sleep 2000
        test <@ ra.Count >= cluster.Workers.Length @>

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
        let logCount = ra |> Seq.filter (fun e -> e.Message.Contains "Work item") |> Seq.length
        test <@ logCount = 2000 @>

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
            raises<FaultException> <@ t.Result @>)

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
            raises<FaultException> <@ t.Result @>)

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
            raises<FaultException> <@ t.Result @>)

    [<Test>]
    member __.``2. Fault Tolerance : fault data`` () =
        test <@ session.Cluster.Run(Cloud.TryGetFaultData()) |> Option.isNone @>

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
            test <@ match t.Result with Some { NumberOfFaults = 1 } -> true | _ -> false @>)

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
            test 
                <@
                    cloudProcess.Result 
                    |> Array.forall (function FaultException _ -> true | _ -> false)
                @>)

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
            test <@ t.Result = 100 @>)

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
            raises<FaultException> <@ t.Result @>
            test <@ t.Status = CloudProcessStatus.Faulted @>)

[<Category("Storage Emulator")>]
type ``Runtime Tests - Standalone Cluster - Storage Emulator`` () =
    inherit ``Azure Runtime Tests``(mkEmulatorConfig (), 4)

[<Category("Standalone Cluster")>]
type ``Runtime Tests - Standalone Cluster - Remote Storage`` () =
    inherit ``Azure Runtime Tests``(mkRemoteConfig (), 4)