namespace MBrace.Azure.Runtime.Tests

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Tests
open MBrace.Azure
open MBrace.Azure
open MBrace.Azure.Runtime

#nowarn "445" // 'Reset'

[<AbstractClass; TestFixture>]
type ``Azure Runtime Tests`` (sbus, storage) as self =
    inherit ``Parallelism Tests`` (parallelismFactor = 4, delayFactor = 10000)
    
    let config = 
        { Configuration.Default with
            StorageConnectionString = storage
            ServiceBusConnectionString = sbus }

    let session = new RuntimeSession(config)

    let run (wf : Cloud<'T>) = self.Run wf
    let repeat f = repeat self.Repeats f

    member __.Configuration = config

    [<TestFixtureSetUp>]
    abstract Init : unit -> unit
    default __.Init () = session.Start()

    [<TestFixtureTearDown>]
    abstract Fini : unit -> unit
    default __.Fini () = session.Stop()

    override __.IsTargetWorkerSupported = true

    override __.Run (workflow : Cloud<'T>) = 
        session.Runtime.RunAsync(workflow)
        |> Async.Catch
        |> Async.RunSync

    override __.Run (workflow : ICloudCancellationTokenSource -> #Cloud<'T>) = 
        async {
            let runtime = session.Runtime
            let cts = runtime.CreateCancellationTokenSource()
            let! ps = runtime.CreateProcessAsync(workflow cts, cancellationToken = cts.Token)
            return! Async.Catch <| ps.AwaitResult()
        } |> Async.RunSync

    override __.RunLocally(workflow : Cloud<'T>) = session.Runtime.RunLocally(workflow)

    override __.Logs = failwith "Not implemented"
    override __.FsCheckMaxTests = 4
    override __.Repeats = 1
    override __.UsesSerialization = true

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

//    [<Test>]
//    member __.``Z5. Fault Tolerance : map/reduce`` () =
//        repeat(fun () ->
//            let runtime = session.Runtime
//            let t = runtime.StartAsTask(WordCount.run 20 WordCount.mapReduceRec)
//            do Thread.Sleep 4000
//            runtime.KillAllWorkers()
//            runtime.AppendWorkers 4
//            t.Result |> shouldEqual 100)
//
//    [<Test>]
//    member __.``Z5. Fault Tolerance : Custom fault policy 1`` () =
//        repeat(fun () ->
//            let runtime = session.Runtime
//            let t = runtime.StartAsTask(Cloud.Sleep 20000, faultPolicy = FaultPolicy.NoRetry)
//            do Thread.Sleep 4000
//            runtime.KillAllWorkers()
//            runtime.AppendWorkers 4
//            Choice.protect (fun () -> t.Result) |> Choice.shouldFailwith<_, FaultException>)
//
//    [<Test>]
//    member __.``Z5. Fault Tolerance : Custom fault policy 2`` () =
//        repeat(fun () ->
//            let runtime = session.Runtime
//            let t = runtime.StartAsTask(Cloud.WithFaultPolicy FaultPolicy.NoRetry (Cloud.Sleep 20000 <||> Cloud.Sleep 20000))
//            do Thread.Sleep 4000
//            runtime.KillAllWorkers()
//            runtime.AppendWorkers 4
//            Choice.protect (fun () -> t.Result) |> Choice.shouldFailwith<_, FaultException>)

type ``Compute - Storage Emulator`` () =
    inherit ``Azure Runtime Tests``(Utils.selectEnv "azureservicebusconn", "UseDevelopmentStorage=true")
    
    [<TestFixtureSetUpAttribute>]
    override __.Init() =
        // TODO : Check if emulator is up?
        base.Init()    

type ``Standalone - Storage Emulator`` () =
    inherit ``Azure Runtime Tests``(Utils.selectEnv "azureservicebusconn", "UseDevelopmentStorage=true")

    [<TestFixtureSetUpAttribute>]
    override __.Init() =
        MBraceAzure.LocalWorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.azureworker.exe"
        MBraceAzure.SpawnLocal(base.Configuration, 4, 16) 
        base.Init()
        
    [<TestFixtureTearDownAttribute>]
    override __.Fini() =
        System.Diagnostics.Process.GetProcessesByName "MBrace.Azure.Runtime.Standalone"
        |> Seq.iter (fun ps -> ps.Kill())
        base.Fini()


type ``Standalone`` () =
    inherit ``Azure Runtime Tests``(Utils.selectEnv "azureservicebusconn", Utils.selectEnv "azurestorageconn")
    
    [<TestFixtureSetUpAttribute>]
    override __.Init() =
        MBraceAzure.LocalWorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.azureworker.exe"
        MBraceAzure.SpawnLocal(base.Configuration, 4, 16) 
        base.Init()
        
    [<TestFixtureTearDownAttribute>]
    override __.Fini() =
        System.Diagnostics.Process.GetProcessesByName "MBrace.Azure.Runtime.Standalone"
        |> Seq.iter (fun ps -> ps.Kill())
        base.Fini()