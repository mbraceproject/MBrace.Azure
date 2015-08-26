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
type ``Azure Cloud Tests`` (sbus, storage) as self =
    inherit ``Cloud Tests`` (parallelismFactor = 20, delayFactor = 15000)
    
    let config = new Configuration(storage, sbus)

    let session = new RuntimeSession(config)

    let run (wf : Cloud<'T>) = self.RunOnCloud wf

    member __.Configuration = config

    [<TestFixtureSetUp>]
    abstract Init : unit -> unit
    default __.Init () = session.Start()

    [<TestFixtureTearDown>]
    abstract Fini : unit -> unit
    default __.Fini () = session.Stop()

    override __.IsTargetWorkerSupported = true
    override __.IsSiftedWorkflowSupported = true

    override __.RunOnCloud (workflow : Cloud<'T>) = 
        session.Runtime.RunOnCloudAsync(workflow)
        |> Async.Catch
        |> Async.RunSync

    override __.RunOnCloud (workflow : ICloudCancellationTokenSource -> #Cloud<'T>) = 
        async {
            let runtime = session.Runtime
            let cts = runtime.CreateCancellationTokenSource()
            let! ps = runtime.CreateCloudTaskAsync(workflow cts, cancellationToken = cts.Token)
            return! Async.Catch <| ps.AwaitResult()
        } |> Async.RunSync

    override __.RunOnCloudWithLogs (workflow : Cloud<unit>) : string [] =
        async {
            let runtime = session.Runtime
            let! t = runtime.CreateCloudTaskAsync(workflow)
            do! t.AwaitResult()
            return t.GetLogs () |> Array.map CloudLogEntry.Format
        } |> Async.RunSync

    override __.RunOnCurrentProcess(workflow : Cloud<'T>) = session.Runtime.RunOnCurrentProcess(workflow)

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
    inherit ``Azure Cloud Tests``(Utils.selectEnv "azureservicebusconn", "UseDevelopmentStorage=true")
    
    [<TestFixtureSetUpAttribute>]
    override __.Init() =
        // TODO : Check if emulator is up?
        base.Init()    

type ``Standalone - Storage Emulator`` () =
    inherit ``Azure Cloud Tests``(Utils.selectEnv "azureservicebusconn", "UseDevelopmentStorage=true")

    [<TestFixtureSetUpAttribute>]
    override __.Init() =
        MBraceAzure.LocalWorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.azureworker.exe"
        MBraceAzure.SpawnLocal(base.Configuration, 4, 32) 
        base.Init()
        
    [<TestFixtureTearDownAttribute>]
    override __.Fini() =
        System.Diagnostics.Process.GetProcessesByName "MBrace.Azure.Runtime.Standalone"
        |> Seq.iter (fun ps -> ps.Kill())
        base.Fini()


type ``Standalone`` () =
    inherit ``Azure Cloud Tests``(Utils.selectEnv "azureservicebusconn", Utils.selectEnv "azurestorageconn")
    
    [<TestFixtureSetUpAttribute>]
    override __.Init() =
        MBraceAzure.LocalWorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.azureworker.exe"
        MBraceAzure.SpawnLocal(base.Configuration, 4, 32) 
        base.Init()
        
    [<TestFixtureTearDownAttribute>]
    override __.Fini() =
        System.Diagnostics.Process.GetProcessesByName "MBrace.Azure.Runtime.Standalone"
        |> Seq.iter (fun ps -> ps.Kill())
        base.Fini()