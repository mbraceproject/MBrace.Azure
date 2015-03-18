#I "../../bin/"
#r "MBrace.Core.dll"
#r "FsPickler.dll"
#r "Vagabond.dll"
#r "MBrace.Azure.Runtime.dll"
#r "MBrace.Azure.Client.dll"
#time "on"

open MBrace
open MBrace.Azure
open MBrace.Azure.Client
open System

Runtime.LocalWorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.azureworker.exe"

let selectEnv name =
    (Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.User),
      Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.Machine),
        Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.Process))
    |> function 
       | s, _, _ when not <| String.IsNullOrEmpty(s) -> s
       | _, s, _ when not <| String.IsNullOrEmpty(s) -> s
       | _, _, s when not <| String.IsNullOrEmpty(s) -> s
       | _ -> failwith "Variable not found"

let config = 
    { Configuration.Default with
        StorageConnectionString = selectEnv "azurestorageconn"
        ServiceBusConnectionString = selectEnv "azureservicebusconn" }


let runtime = Runtime.GetHandle(config)
runtime.AttachClientLogger(new ConsoleLogger())
//runtime.Reset(reactivate = false)
//runtime.Reset()

// local only---
Runtime.SpawnLocal(config, 4, 16)
// ----------------------------


runtime.ShowProcesses()
runtime.ShowWorkers()
runtime.ShowLogs()
runtime.GetWorkers() 
|> Seq.iter (fun w -> printfn "%s %s %f" w.Id w.Hostname w.Memory)

runtime.ClearAllProcesses()


let ps = runtime.CreateProcess([1..1000] |> Seq.map (fun i -> cloud { return i*i }) |> Cloud.Parallel)
ps.ShowInfo()
ps.AwaitResult()



let ps =
    cloud {
        return! [1..30]
                |> Seq.map (fun i -> cloud { return i * i })
                |> Cloud.Parallel
    }
    |> runtime.CreateProcess

ps.ShowInfo()
ps.AwaitResult()
ps.Kill()


let ps () = 
 cloud { let tasks = new ResizeArray<_>()
         for i in [ 0 .. 200 ] do 
             let! x = Cloud.StartAsCloudTask (cloud { do! Cloud.Sleep 1000
                                                      return 1 })
             tasks.Add x
         for t in tasks.ToArray() do 
             let! res = t.AwaitResult()
             ()
        }

let job = 
   cloud { return! ps() }
     |> runtime.CreateProcess


job.ShowInfo()



runtime.Run(Cloud.ParallelEverywhere(cloud { return System.Diagnostics.Process.GetCurrentProcess().Id }))


runtime.Run <| Cloud.ParallelEverywhere Cloud.CurrentWorker


runtime.Run <| cloud { return 42 }

let ct = runtime.CreateCancellationTokenSource()
let ctask = runtime.Run(Cloud.StartAsCloudTask(cloud { return 42 }, cancellationToken = ct.Token))

ctask.Result
ctask.Id


let x = 
    cloud {
        let! ctask = Cloud.StartAsCloudTask(cloud { return 42 })
        return! ctask.AwaitResult()
    } |> runtime.Run


let wf = cloud {
    let! sp, rp = CloudChannel.New<int>()
    do! cloud {
            for i = 0 to 10 do
                do! CloudChannel.Send(sp, i)
                printfn "send %d" i
            return ()
        } 
        <||>
        cloud {
            let i = ref 0
            while i.Value <> 10 do
                let! x = CloudChannel.Receive(rp)
                printfn "recv %d" x
                i := x
        } |> Cloud.Ignore
    do! rp.Dispose()
}

runtime.Run wf

let wf = cloud {
    let! atom = CloudAtom.New(42)
    do! [1..10] 
        |> Seq.map (fun _ -> CloudAtom.Update(atom, fun x -> x + 1))
        |> Cloud.Parallel
        |> Cloud.Ignore
    return atom
}

let atom = runtime.Run(wf)
atom.Value |> runtime.RunLocal

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

open MBrace.Store

let ps = runtime.CreateProcess(CloudCell.New(42))
let c = ps.AwaitResult()

c.Value |> runtime.RunLocal
c.Size |> runtime.RunLocal

let ps = runtime.CreateProcess(CloudSequence.New([42]))
let cr = ps.AwaitResult()
cr.Count |> runtime.RunLocal
cr.Size |> runtime.RunLocal
cr.ToEnumerable() |> runtime.RunLocal


runtime.StoreClient.CloudCell.Read(c)




cloud { 
    return! cloud { return 1 } <||> cloud { return 2 }
}
|> runtime.RunLocal


let c = ref 0
for i in 1 .. 10 do
    c := runtime.Run(cloud { return !c + 1 })


let x = cloud { return 42 }


runtime.Run x