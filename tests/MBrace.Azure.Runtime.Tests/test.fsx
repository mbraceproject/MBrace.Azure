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

//Configuration.Activate(config) |> Async.RunSync
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
    Cloud.Choice 
        [|  cloud { return if i = max then Some 42 else None }
            cloud { return! wf (i+1) max }
            cloud { return! wf (i+1) max }
        |]

let ps = runtime.CreateProcess (wf 0 2)
ps.AwaitResult() 
ps.ClearProcessResources()






[<AutoOpen>]
module FaultPolicyExtensions =
    type FaultPolicyBuilder (debug, fp : FaultPolicy) =
        inherit CloudBuilder()

        member __.Run(wf : Cloud<'T>) = 
            cloud {
                let! handle = cloud { printfn "%s" debug
                                      return! wf }
                              |> Cloud.StartChild
                              |> Cloud.WithFaultPolicy fp
                return! handle
            }

    let exactlyOnce = new FaultPolicyBuilder("exactlyOnce", FaultPolicy.NoRetry) //:> CloudBuilder
    let retry n = new FaultPolicyBuilder("retry n", FaultPolicy.Retry(n)) //:> CloudBuilder
    let infinite = new FaultPolicyBuilder("infinite", FaultPolicy.InfiniteRetry()) //:> CloudBuilder

let wf = exactlyOnce {
    let! x = infinite { 
                do! Cloud.Sleep 10000 
                return 42
            }
    let! z = retry 3 {
                do! Cloud.Sleep 10000
                return 44
            }
    let! y = exactlyOnce { 
                do! Cloud.Sleep 10000
                return 43 
            }
    return x, z, y
}




let ps = runtime.CreateProcess(wf, faultPolicy = FaultPolicy.NoRetry)
ps.AwaitResult()
