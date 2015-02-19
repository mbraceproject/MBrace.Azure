namespace MBrace.Azure.Runtime.Tests

open System
open System.Threading

open NUnit.Framework
open FsUnit

open MBrace
open MBrace.Workflows
open MBrace.Continuation
open MBrace.Azure.Client
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Standalone
open MBrace.Azure.Runtime.Resources

[<AutoOpen>]
module Helpers =
    [<Literal>]
#if DEBUG
    let repeats = 3
#else
    let repeats = 1
#endif

    type Counter with
        member l.Incr() = l.Increment() |> Async.RunSync
           
    let wordCount size mapReduceAlgorithm : Cloud<int> =
        let mapF (text : string) = cloud { return text.Split(' ').Length }
        let reduceF i i' = cloud { return i + i' }
        let inputs = Array.init size (fun i -> "lorem ipsum dolor sit amet")
        mapReduceAlgorithm mapF reduceF 0 inputs

    let rec mapReduceRec (mapF : 'T -> Cloud<'S>) 
                         (reduceF : 'S -> 'S -> Cloud<'S>)
                         id
                         (inputs : 'T []) =
        cloud {
            match inputs with
            | [||] -> return id
            | [|t|] -> return! mapF t
            | _ ->
                let left = inputs.[.. inputs.Length / 2 - 1]
                let right = inputs.[inputs.Length / 2 ..]
                let! s,s' = (mapReduceRec mapF reduceF id left) <||> (mapReduceRec mapF reduceF id right)
                return! reduceF s s'
        }


[<AbstractClass; TestFixture>]
type ``Azure Runtime Tests`` (sbus, storage) =
    let config = 
        { Configuration.Default with
            StorageConnectionString = storage
            ServiceBusConnectionString = sbus }
    
    let testContainer = "tests"
    let mutable runtime : Runtime option = None
    let configId : ConfigurationId = config.ConfigurationId
    let processes = new ResizeArray<Process>()

    let run (workflow : Cloud<'T>) = 
        let ps = Option.get(runtime).CreateProcess(workflow) 
        processes.Add(ps)
        ps.AwaitResultAsync() |> Async.Catch |> Async.RunSync

    let runCts (workflow : DistributedCancellationTokenSource -> Cloud<'T>) = 
        async {
            let runtime = Option.get runtime
            let! dcts = DistributedCancellationTokenSource.Create(configId, testContainer) 
            let ct = dcts.GetLocalCancellationToken()
            let ps = runtime.CreateProcess(workflow dcts, cancellationToken = ct) 
            processes.Add(ps)
            return! ps.AwaitResultAsync() |> Async.Catch 
        } |> Async.RunSync
    
    member __.Configuration = config

    abstract Init : unit -> unit
    [<TestFixtureSetUp>]
    default __.Init () =
        runtime <- Some <| MBrace.Azure.Client.Runtime.GetHandle(config)
        printfn "Got Handle for runtime"
        runtime.Value.AttachLogger(new Common.ConsoleLogger()) 

    [<TestFixtureTearDown>]
    member __.Fini () =
        [for p in processes -> runtime.Value.ClearProcessAsync(p.Id) ]
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSync
        runtime <- None


    [<Test>]
    member __.``1. Parallel : empty input`` () =
        run (Cloud.Parallel Seq.empty<Cloud<int>>) |> Choice.shouldEqual [||]

    [<Test>]
    member __.``1. Parallel : simple inputs`` () =
        cloud {
            let f i = cloud { return i + 1 }
            let! results = Array.init 20 f |> Cloud.Parallel
            return Array.sum results
        } |> run |> Choice.shouldEqual 210

    [<Test>]
    member __.``1. Parallel : use binding`` () =
        let counter = Counter.Create(configId, testContainer, 0) |> Async.RunSync
        cloud {
            use foo = { new ICloudDisposable with member __.Dispose () = cloud { return counter.Incr() |> ignore } }
            let! _ = cloud { return counter.Incr() } <||> cloud { return counter.Incr() }
            return counter.Value
        } |> run |> Choice.shouldEqual 2

        counter.Value |> should equal 3

    [<Test>]
    member __.``1. Parallel : exception handler`` () =
        cloud {
            try
                let! x,y = cloud { return 1 } <||> cloud { return invalidOp "failure" }
                return x + y
            with :? InvalidOperationException as e ->
                let! x,y = cloud { return 1 } <||> cloud { return 2 }
                return x + y
        } |> run |> Choice.shouldEqual 3

    [<Test>]
    member __.``1. Parallel : finally`` () =
        let counter = Counter.Create(configId, testContainer, 0) |> Async.RunSync
        cloud {
            try
                let! x,y = cloud { return 1 } <||> cloud { return invalidOp "failure" }
                return x + y
            finally
                counter.Incr () |> ignore
        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>

        counter.Value |> should equal 1

    [<Test>]
    member __.``1. Parallel : simple nested`` () =
        cloud {
            let f i j = cloud { return i + j + 2 }
            let cluster i = Array.init 10 (f i) |> Cloud.Parallel
            let! results = Array.init 10 cluster |> Cloud.Parallel
            return Array.concat results |> Array.sum
        } |> run |> Choice.shouldEqual 1100

    [<Test>]
    member __.``1. Parallel : simple exception`` () =
        cloud {
            let f i = cloud { return if i = 15 then invalidOp "failure" else i + 1 }
            let! results = Array.init 20 f |> Cloud.Parallel
            return Array.sum results
        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>


    [<Test>]
    [<Repeat(repeats)>]
    member __.``1. Parallel : exception contention`` () =
        let counter = Counter.Create(configId, testContainer, 0) |> Async.RunSync
        cloud {
            try
                let! _ = Array.init 20 (fun _ -> cloud { return invalidOp "failure" }) |> Cloud.Parallel
                return raise <| new exn("Cloud.Parallel should not have completed succesfully.")
            with :? InvalidOperationException ->
                counter.Incr() |> ignore
                return ()
        } |> run |> Choice.shouldEqual ()

        // test that exception continuation was fired precisely once
        counter.Value |> should equal 1


    [<Test>]
    [<Repeat(repeats)>]
    member __.``1. Parallel : exception cancellation`` () =
        cloud {
            let worker i = cloud { 
                if i = 0 then
                    invalidOp "failure"
                else
                    return ()
            }


            let! _ = Array.init 20 worker |> Cloud.Parallel
            return raise <| new exn("Cloud.Parallel should not have completed succesfully.")
        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>

    [<Test>]
    [<Repeat(repeats)>]
    member __.``1. Parallel : nested exception cancellation`` () =
        cloud {
            let worker i j = cloud {
                if i = 0 && j = 0 then
                    invalidOp "failure"
                else
                    return ()
            }

            let cluster i = Array.init 5 (worker i) |> Cloud.Parallel |> Cloud.Ignore
            do! Array.init 5 cluster |> Cloud.Parallel |> Cloud.Ignore
            return raise <| new AssertionException("Cloud.Parallel should not have completed succesfully.")
        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>

    [<Test>]
    [<Repeat(repeats)>]
    member __.``1. Parallel : simple cancellation`` () =
        let counter = Counter.Create(configId, testContainer, 0) |> Async.RunSync
        runCts(fun cts -> cloud {
            let f _ = cloud {
                if counter.Incr() = 1 then cts.Cancel() 
                else return! Cloud.Sleep 1000
            }

            let! _ = Array.init 10 f |> Cloud.Parallel

            return ()
        }) |> Choice.shouldFailwith<_, OperationCanceledException>


    [<Test>]
    [<Repeat(repeats)>]
    member __.``1. Parallel : to local`` () =
        // check local semantics are forced by using ref cells.
        cloud {
            let counter = ref 0
            let seqWorker _ = cloud {
                Interlocked.Increment counter |> ignore
            }

            let! results = Array.init 20 seqWorker |> Cloud.Parallel |> Cloud.ToLocal
            return counter.Value
        } |> run |> Choice.shouldEqual 20

    [<Test>]
    [<Repeat(repeats)>]
    member __.``1. Parallel : to sequential`` () =
        // check sequential semantics are forced by deliberately
        // making use of code that is not thread-safe.
        cloud {
            let counter = ref 0
            let seqWorker _ = cloud {
                let init = counter.Value + 1
                counter := init
                return counter.Value = init
            }

            let! results = Array.init 20 seqWorker |> Cloud.Parallel |> Cloud.ToSequential
            return Array.forall id results
        } |> run |> Choice.shouldEqual true

    [<Test>]
    [<Repeat(repeats)>]
    member __.``1. Parallel : recursive map/reduce`` () =
        wordCount 20 mapReduceRec |> run |> Choice.shouldEqual 100

    [<Test>]
    [<Repeat(repeats)>]
    member __.``1. Parallel : balanced map/reduce`` () =
        wordCount 1000 Workflows.Distributed.mapReduce |> run |> Choice.shouldEqual 5000
        
    [<Test>]
    member __.``2. Choice : empty input`` () =
        Cloud.Choice Seq.empty<Cloud<int option>> |> run |> Choice.shouldEqual None

    [<Test>]
    [<Repeat(repeats)>]
    member __.``2. Choice : all inputs 'None'`` () =
        cloud {
            let worker _ = cloud {
                return None
            }

            let! result = Array.init 20 worker |> Cloud.Choice
            return result
        } |> run |> Choice.shouldEqual None


    [<Test>]
    [<Repeat(repeats)>]
    member __.``2. Choice : one input 'Some'`` () =
        cloud {
            let worker i = cloud {
                if i = 0 then return Some i
                else
                    return None
            }

            let! result = Array.init 20 worker |> Cloud.Choice
            return result
        } |> run |> Choice.shouldEqual(Some 0)

    [<Test>]
    [<Repeat(repeats)>]
    member __.``2. Choice : all inputs 'Some'`` () =
        let successcounter = Counter.Create(configId, testContainer, 0) |> Async.RunSync
        cloud {
            let worker _ = cloud { return Some 42 }
            let! result = Array.init 20 worker |> Cloud.Choice
            let _ = successcounter.Incr()
            return result
        } |> run |> Choice.shouldEqual (Some 42)

        // ensure only one success continuation call
        successcounter.Value |> should equal 1

    [<Test>]
    [<Repeat(repeats)>]
    member __.``2. Choice : simple nested`` () =
        cloud {
            let worker i j = cloud {
                if i = 3 && j = 3 then
                    return Some(i,j)
                else
                    return None
            }

            let cluster i = Array.init 4 (worker i) |> Cloud.Choice
            let! result = Array.init 5 cluster |> Cloud.Choice
            return result
        } |> run |> Choice.shouldEqual (Some (3,3))


    [<Test>]
    [<Repeat(repeats)>]
    member __.``2. Choice : nested exception cancellation`` () =
        cloud {
            let worker i j = cloud {
                if i = 0 && j = 0 then
                    return invalidOp "failure"
                else
                    do! Cloud.Sleep 1000
                    return None
            }

            let cluster i = Array.init 5 (worker i) |> Cloud.Choice
            return! Array.init 4 cluster |> Cloud.Choice
        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>

    [<Test>]
    [<Repeat(repeats)>]
    member __.``2. Choice : simple cancellation`` () =
        let mutex = Counter.Create(configId, testContainer, 0) |> Async.RunSync
        let taskCount = Counter.Create(configId, testContainer, 0) |> Async.RunSync
        runCts(fun cts ->
            cloud {
                let worker _ = cloud {
                    if mutex.Incr() = 1 then cts.Cancel()
                    do! Cloud.Sleep 3000
                    let _ = taskCount.Incr()
                    return Some 42
                }

                return! Array.init 10 worker |> Cloud.Choice
        }) |> Choice.shouldFailwith<_, OperationCanceledException>

        taskCount.Value |> should equal 0

    [<Test>]
    [<Repeat(repeats)>]
    member __.``2. Choice : to local`` () =
        // check local semantics are forced by using ref cells.
        cloud {
            let counter = ref 0
            let seqWorker i = cloud {
                if i = 16 then
                    do! Cloud.Sleep 100
                    return Some i
                else
                    let _ = Interlocked.Increment counter
                    return None
            }

            let! result = Array.init 20 seqWorker |> Cloud.Choice |> Cloud.ToLocal
            return result, counter.Value
        } |> run |> Choice.shouldEqual (Some 16, 19)

    [<Test>]
    [<Repeat(repeats)>]
    member __.``2. Choice : to sequential`` () =
        // check sequential semantics are forced by deliberately
        // making use of code that is not thread-safe.
        cloud {
            let counter = ref 0
            let seqWorker i = cloud {
                let init = counter.Value + 1
                counter := init
                counter.Value |> should equal init
                if i = 16 then
                    return Some ()
                else
                    return None
            }

            let! result = Array.init 20 seqWorker |> Cloud.Choice |> Cloud.ToSequential
            return result, counter.Value
        } |> run |> Choice.shouldEqual (Some(), 17)


    [<Test>]
    [<Repeat(repeats)>]
    member __.``3. StartChild: task with success`` () =
        let count = Counter.Create(configId, testContainer, 0) |> Async.RunSync
        cloud {
            let task = cloud {
                return count.Incr()
            }

            let! ch = Cloud.StartChild(task)
            return! ch
        } |> run |> Choice.shouldEqual 1

    [<Test>]
    [<Repeat(repeats)>]
    member __.``3. StartChild: task with exception`` () =
        cloud {
            let task = cloud {
                return invalidOp "failure"
            }

            let! ch = Cloud.StartChild(task)
            return! ch
        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>

    [<Test>]
    [<Repeat(repeats)>]
    member __.``3. StartChild: task with cancellation`` () =
        let count = Counter.Create(configId, testContainer, 0) |> Async.RunSync
        runCts(fun cts ->
            cloud {
                let task = cloud {
                    let _ = count.Incr()
                    do! Cloud.Sleep 3000
                    return count.Incr()
                }

                let! ch = Cloud.StartChild(task)
                do! Cloud.Sleep 1000
                cts.Cancel ()
                return! ch
        }) |> Choice.shouldFailwith<_, OperationCanceledException>

        // ensure final increment was cancelled.
        count.Value |> should equal 1


    [<Test>]
    member __.``4. Runtime : Get worker count`` () =
        run (Cloud.GetWorkerCount()) |> Choice.shouldEqual (runtime.Value.GetWorkers() |> Seq.length)

    [<Test>]
    member __.``4. Runtime : Get current worker`` () =
        run Cloud.CurrentWorker |> Choice.shouldMatch (fun _ -> true)

    [<Test>]
    member __.``4. Runtime : Get process id`` () =
        run (Cloud.GetProcessId()) |> Choice.shouldMatch (fun _ -> true)

    [<Test>]
    member __.``4. Runtime : Get job id`` () =
        run (Cloud.GetJobId()) |> Choice.shouldMatch (fun _ -> true)


type ``Compute Emulator`` () =
    inherit ``Azure Runtime Tests``(Utils.selectEnv "azureservicebusconn", "UseDevelopmentStorage=true")
    
    [<TestFixtureSetUpAttribute>]
    override __.Init() =
        // TODO : Check if emulator is up?
        base.Init()    

type ``InitLocal`` () =
    inherit ``Azure Runtime Tests``(Utils.selectEnv "azureservicebusconn", Utils.selectEnv "azurestorageconn")
    
    [<TestFixtureSetUpAttribute>]
    override __.Init() =
        Runtime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.Azure.Runtime.Standalone.exe"
        Runtime.Spawn(base.Configuration, 4, 16) 
        base.Init()    
