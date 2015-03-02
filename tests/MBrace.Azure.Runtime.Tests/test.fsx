#I "../../bin/"
#r "MBrace.Core.dll"
#r "FsPickler.dll"
#r "Vagabond.dll"
#r "MBrace.Azure.Runtime.Common.dll"
#r "MBrace.Azure.Runtime.dll"
#r "MBrace.Azure.Client.dll"
#time "on"

open MBrace
open MBrace.Azure
open MBrace.Azure.Client
open System

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

// local only---
#r "MBrace.Azure.Runtime.Standalone"
open MBrace.Azure.Runtime.Standalone
Runtime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.Azure.Runtime.Standalone.exe"
Runtime.Spawn(config, 4, 16)
// ----------------------------

let runtime = Runtime.GetHandle(config)
runtime.AttachClientLogger(new TimeEllapsedLogger(new ConsoleLogger()))
//runtime.Reset(reactivate = false)
//runtime.Reset()


type Foo () =
    member __.Bar () = ()

let ps = runtime.Run(cloud { return typeof<Foo>.Assembly.Location })


let ps = 
 [for i in 0 .. 3 ->
   cloud { return System.DateTime.Now }
    |> runtime.CreateProcess ]


let ps = cloud { return 42 } |> runtime.CreateProcess
ps.AwaitResult()

runtime.ShowProcesses()
runtime.ShowWorkers()
runtime.ShowLogs()

runtime.ClearAllProcesses()

let ps =
    [1..30]
    |> Seq.map (fun i -> cloud { return i * i })
    |> Cloud.Parallel
    |> runtime.CreateProcess

ps.ShowInfo()
ps.AwaitResult()

runtime.Run(Cloud.Parallel(cloud { return System.Diagnostics.Process.GetCurrentProcess().Id }))


runtime.Run <| Cloud.Parallel Cloud.CurrentWorker


runtime.Run <| cloud { return 42 }

open MBrace.Workflows

let rec wf i max = 
    cloud { 
        if i = max then return 42 
        else return! wf (i + 1) max <|> wf (i + 1) max
    }

let ps = runtime.CreateProcess(wf 0 3)
ps.ShowInfo()
ps.AwaitResult() 

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

let ps = runtime.CreateProcess(CloudRef.New(42))
let c = ps.AwaitResult()

c.Value |> runtime.RunLocal
c.Size |> runtime.RunLocal

let ps = runtime.CreateProcess(CloudSequence.New([42]))
let cr = ps.AwaitResult()
cr.Count |> runtime.RunLocal
cr.Size |> runtime.RunLocal
cr.ToEnumerable() |> runtime.RunLocal


runtime.DefaultStoreClient.CloudRef.Read(c)




cloud { 
    return! cloud { return 1 } <||> cloud { return 2 }
}
|> runtime.RunLocal


