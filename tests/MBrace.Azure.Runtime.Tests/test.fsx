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
open Nessos.MBrace.Runtime
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Client
open System
open System.Threading

let selectEnv name =
    (Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.User),
        Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.Machine))
    |> function | null, s | s, null | s, _ -> s

let config = 
    { Configuration.Default with
        StorageConnectionString = selectEnv "azurestorageconn"
        ServiceBusConnectionString = selectEnv "azureservicebusconn"  }

//Configuration.Activate(config)
//Configuration.DeleteConfigurationResources(config)

// local only---
#r "MBrace.Azure.Runtime.Standalone"
open Nessos.MBrace.Azure.Runtime.Standalone
Runtime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.Azure.Runtime.Standalone.exe"
Runtime.Spawn(config, 4)
// inmemory-----
[1..4]
|> List.map(fun _ -> 
    let svc = Service(config, 10)
    svc.AttachLogger(Nessos.MBrace.Azure.Runtime.Common.ConsoleLogger())
    svc.StartAsync())
|> Async.Parallel
|> Async.Ignore
|> Async.Start
//--------------

let runtime = Runtime.GetHandle(config)
//runtime.ClientLogger.Attach(new Nessos.MBrace.Azure.Runtime.Common.ConsoleLogger()) 

runtime.ShowProcesses()
runtime.ShowWorkers()
runtime.ShowLogs()

let rec loop i = cloud {
    printfn "%d" i
    do! Cloud.Sleep 1000
    if i > 100 then return ()
    else return! loop(i + 2)
}

runtime.Run(Cloud.Parallel [| loop 0; loop 1|])

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

let x = runtime.Run(f 10)


runtime.Run(Cloud.Choice <| List.init 100 (fun i -> cloud { return if i = 82 then Some 42 else None } ))


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
wordCount 1000 Library.MapReduce.mapReduce 
|> runtime.Run

let f =
    cloud {
        let worker i = cloud { 
            if i = 0 then
                invalidOp "failure"
            else
                return ()
        }
        try
            let! _ = Array.init 20 worker |> Cloud.Parallel
            return raise <| new exn("Cloud.Parallel should not have completed succesfully.")
        with :? InvalidOperationException ->
            return 42
} 

while true do
    let ps = runtime.CreateProcess f
    ps.AwaitResult()


