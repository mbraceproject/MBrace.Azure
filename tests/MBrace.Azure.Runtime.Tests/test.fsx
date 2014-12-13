#I "../../bin/"
#r "MBrace.Core.dll"
#r "MBrace.Library.dll"
#r "FsPickler.dll"
#r "Vagrant.dll"
#r "MBrace.Azure.Runtime.Common.dll"
#r "MBrace.Azure.Runtime.dll"
#r "MBrace.Azure.Client.dll"
#time "on"

open Nessos.MBrace
open Nessos.MBrace.Continuation
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Client
open System
open System.IO

let selectEnv name =
    (Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.User),
        Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.Machine))
    |> function | null, s | s, null | s, _ -> s

let config = 
    { Configuration.Default with
        StorageConnectionString = selectEnv "azurestorageconn"
        ServiceBusConnectionString = selectEnv "azureservicebusconn" }

//Configuration.Activate(config) |> Async.RunSync
//Configuration.DeleteResources(config) |> Async.RunSync

// local only---
#r "MBrace.Azure.Runtime.Standalone"
open Nessos.MBrace.Azure.Runtime.Standalone
Runtime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.Azure.Runtime.Standalone.exe"
Runtime.Spawn(config, 4)
// inmemory-----
//[1..1]
//|> List.map(fun _ -> 
//    let svc = Service(config, 10)
//    svc.AttachLogger(Nessos.MBrace.Azure.Runtime.Common.ConsoleLogger())
//    svc.StartAsync())
//|> Async.Parallel
//|> Async.Ignore
//|> Async.Start
//--------------

let runtime = Runtime.GetHandle(config)
runtime.ClientLogger.Attach(new Common.ConsoleLogger()) 

runtime.ShowProcesses()
runtime.ShowWorkers()
runtime.ShowLogs()

let rec loop i = cloud {
    printfn "%d" i
    do! Cloud.Sleep 1000
    if i > 100 then return ()
    else return! loop(i + 2)
}

let wf = Cloud.Parallel [| loop 0; loop 1|]

runtime.CreateProcess wf

let ps = runtime.CreateProcess(cloud { return 42 }, name = "foo")
ps.AwaitResult()

let p = runtime.GetProcess(ps.Id)
p

runtime.Run <| Cloud.Log "FOOO"

runtime.Run(cloud { return failwith<int> "foo" })
runtime.Run <| Cloud.GetWorkerCount()
runtime.Run <| Cloud.CurrentWorker

let ps = runtime.CreateProcess <| Cloud.Log "FOO"
ps.ShowLogs()

let f i = Cloud.Parallel <| List.init i (fun x -> cloud { return x+1 })

let x = runtime.CreateProcess(f 10)


let p = runtime.CreateProcess(Cloud.Choice <| List.init 100 (fun i -> cloud { return if i = 42 then Some i else None } ))


type Foo = { Value : int }

let ps = runtime.CreateProcess(cloud { return { Value = 42 } })
ps.AwaitResult()

runtime.ShowProcesses()
let p = runtime.GetProcess("6043e2f52ffe4c888636b3efdc6c7f3f")
p.AwaitResultBoxed()


let wordCount size mapReduceAlgorithm : Cloud<int> =
    let mapF (text : string) = cloud { return text.Split(' ').Length }
    let reduceF i i' = cloud { return i + i' }
    let inputs = Array.init size (fun i -> "lorem ipsum dolor sit amet")
    mapReduceAlgorithm mapF 0 reduceF inputs
wordCount 1000 MapReduce.mapReduce 
|> runtime.Run


let ws = runtime.GetWorkers() |> Seq.toArray
let wr = ws.[0]

runtime.Run <| cloud { let! child = Cloud.StartChild(Cloud.Log "FOO", wr) in return! child }


let atom = runtime.Run <| CloudAtom.New(12)

atom.GetValue() |> Async.RunSync
atom.Update((+) 1) |> Async.RunSync

runtime.Run <| CloudAtom.Transact(fun x -> x+1, x + 1) atom

atom.Id

let cf = runtime.Run <| CloudFile.New((fun stream -> async { use sr = new StreamWriter(stream) in return sr.WriteLine("Foobar") }), "foo/bar")
cf.FileName

let s = cf.BeginRead() |> Async.RunSync
let sr = new StreamReader(s)
sr.ReadToEnd()
s.Dispose()

runtime.Run <| FileStore.EnumerateDirectories()
runtime.Run <| FileStore.CreateDirectory("foo")


let sp, rp = runtime.Run <| CloudChannel.New<int>()

let ps = runtime.CreateProcess(
                    cloud { 
                        while true do 
                            let! x = CloudChannel.Receive(rp)
                            printfn "Got %d" x })

for i = 0 to 100 do
    sp.Send(i) |> Async.RunSync


runtime.Run(cloud { return DateTime.Now })


let cref = runtime.Run <| CloudRef.New(42)

let wf = cloud {
    let! cseq = CloudSeq.New([1..10])
    let! ch = Cloud.StartChild(cloud { return Seq.toArray cseq })
    return! ch
}

let x = runtime.Run wf
runtime.Run <| CloudSeq.New [1..10]

let wf =
    cloud { do! Cloud.Log "Foo" 
            return! Cloud.Sleep 20000 }

let t1 = runtime.CreateProcess(wf, faultPolicy = FaultPolicy.NoRetry)
t1.AwaitResult()
let t2 = runtime.CreateProcess(wf,faultPolicy = FaultPolicy.Retry(1))
t2.AwaitResult()


let t3 = runtime.CreateProcess(Cloud.WithFaultPolicy FaultPolicy.NoRetry (Cloud.Sleep 20000 <||> Cloud.Sleep 20000))

runtime.ShowProcesses()