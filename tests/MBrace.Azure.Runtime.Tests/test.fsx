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

let selectEnv name =
    (Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.User),
        Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.Machine))
    |> function | null, s | s, null | s, _ -> s

let config = 
    { Configuration.Default with
        StorageConnectionString = selectEnv "azurestorageconn"
        ServiceBusConnectionString = selectEnv "azureservicebusconn" }


//Configuration.Activate(config)
//Configuration.DeleteResources(config) |> Async.RunSync

// local only---
#r "MBrace.Azure.Runtime.Standalone"
open Nessos.MBrace.Azure.Runtime.Standalone
Runtime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.Azure.Runtime.Standalone.exe"
Runtime.Spawn(config, 4, 16)
// ----------------------------

let runtime = Runtime.GetHandle(config)
runtime.ClientLogger.Attach(new Common.ConsoleLogger()) 

runtime.ShowProcesses()
runtime.ShowWorkers()
runtime.ShowLogs()

let rec wf i max = 
    cloud { 
        if i = max then return 42 
        else return! wf (i + 1) max <|> wf (i + 1) max
    }

let ps = runtime.CreateProcess(wf 0 3)
ps.ShowInfo()
ps.AwaitResult() 
ps.ClearProcessResources()


let sc = runtime.StoreClient

let sp, rp = sc.CloudChannel.New<int>() |> Async.RunSync

sp.Send(43) |> Async.RunSync
rp.Receive() |> Async.RunSync

let wf = cloud {
    let! sp, rp = CloudChannel.New<int>()
    do! cloud {
            for i = 0 to 10 do
                do! Cloud.Sleep 1000
                do! CloudChannel.Send i sp
                printfn "send %d" i
            return ()
        } 
        <||>
        cloud {
            let i = ref 0
            while i.Value <> 10 do
                let! x = CloudChannel.Receive rp
                printfn "recv %d" x
                i := x
        } |> Cloud.Ignore
    do! Cloud.OfAsync <| rp.Dispose()
}

let wf = cloud {
    let! atom = CloudAtom.New(42)
    do! [1..10] 
        |> Seq.map (fun _ -> CloudAtom.Update(fun x -> x + 1) atom)
        |> Cloud.Parallel
        |> Cloud.Ignore
    return atom
}

let atom = runtime.Run(wf)
atom.Value

[<AutoOpen>]
module FaultPolicyExtensions =
    type FaultPolicyBuilder (fp : FaultPolicy) =
        inherit CloudBuilder()

        member __.Run(wf : Cloud<'T>) = 
            cloud {
                let! handle = wf
                              |> Cloud.StartChild
                              |> Cloud.WithFaultPolicy fp
                return! handle
            }

    let exactlyOnce = new FaultPolicyBuilder(FaultPolicy.NoRetry) //:> CloudBuilder
    let retry n = new FaultPolicyBuilder(FaultPolicy.Retry(n)) //:> CloudBuilder
    let infinite = new FaultPolicyBuilder(FaultPolicy.InfiniteRetry()) //:> CloudBuilder

let wf = cloud {
    let! x = infinite { 
                printfn "infinite"
                do! Cloud.Sleep 10000 
                return 42
            }
    let! z = retry 3 {
                printfn "retry 3"
                do! Cloud.Sleep 10000
                return 44
            }
    let! y = exactlyOnce { 
                printfn "exactlyOnce"
                do! Cloud.Sleep 10000
                return 43 
            }
    return x, z, y
}




let ps = runtime.CreateProcess(wf)
ps.AwaitResult()

#r "MBrace.Library"
open Nessos.MBrace.Store

let ps = runtime.CreateProcess(CloudSequence.New([42]))
let cr = ps.AwaitResult()
cr |> Seq.toArray
cr.Size