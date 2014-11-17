#I "../../bin/"
#r "MBrace.Core.dll"
#r "MBrace.Library.dll"
#r "Vagrant.dll"
#r "MBrace.Azure.Runtime.exe"
#r "Microsoft.WindowsAzure.Storage.dll"
#r "Microsoft.ServiceBus.dll"
#time "on"

open Nessos.MBrace
open Nessos.MBrace.Runtime
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Config
open Nessos.MBrace.Azure.Runtime.Common
open Nessos.MBrace.Azure.Runtime.Resources
open System
open System.Threading
open System.Threading.Tasks

let selectEnv name =
    (Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.User),
        Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.Machine))
    |> function | null, s | s, null | s, _ -> s

let config = 
    { StorageConnectionString = selectEnv "AzureStorageConn";
        ServiceBusConnectionString = selectEnv "AzureServiceBusConn" }

Config.initialize config
MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.Azure.Runtime.exe"

let runtime = MBraceRuntime.InitLocal(3)

runtime.Run(cloud { return 42 }, cleanup = true)

//runtime.Run <| Cloud.GetWorkerCount()

let f x i = Cloud.Parallel <| List.init i (fun x -> cloud { return x + i })

let x = runtime.Run(f 0 100)


runtime.Run(Cloud.Choice <| List.init 100 (fun i -> cloud { return if i = 12 then Some 42 else None } ))

let cts = new CancellationTokenSource()
let t  = runtime.RunAsTask(cloud { while true do do! Cloud.Sleep 1000 }, cts.Token)
cts.Cancel()

let wordCount size mapReduceAlgorithm : Cloud<int> =
    let mapF (text : string) = cloud { return text.Split(' ').Length }
    let reduceF i i' = cloud { return i + i' }
    let inputs = Array.init size (fun i -> "lorem ipsum dolor sit amet")
    mapReduceAlgorithm mapF 0 reduceF inputs
wordCount 1000 Library.MapReduce.mapReduce 
|> runtime.Run



runtime.KillAllWorkers() 
runtime.AppendWorkers 4





let (!) (task : Async<'T>) = Async.RunSynchronously task

ClientProvider.TableClient.GetTableReference("bootstap").DeleteIfExists()
ClientProvider.BlobClient.GetContainerReference("bootstrap").DeleteIfExists()
Config.ClientProvider.NamespaceClient.DeleteQueue("bootstrap")

Config.ClientProvider.TableClient.ListTables("process")
|> Seq.map (fun t -> t.DeleteAsync() |> Async.AwaitIAsyncResult)
|> Async.Parallel
|> Async.RunSynchronously

Config.ClientProvider.BlobClient.ListContainers("process")
|> Seq.map (fun t -> t.DeleteAsync() |> Async.AwaitIAsyncResult)
|> Async.Parallel
|> Async.RunSynchronously


//-------------------------------------------------------------------

let c = !Counter.Init("tmp", 1)
!c.Increment()

let l = !Latch.Init("tmp", 11)
!l.Decrement()

[|1..5|]
|> Array.map (fun _ -> async { do! l.Decrement() |> Async.Ignore })
|> Async.Parallel
|> Async.Ignore
|> Async.RunSynchronously

l.Value

//-------------------------------------------------------------------

let c = !BlobCell.Init("tmp", fun () -> 42)
!c.GetValue()

//-------------------------------------------------------------------

let q : Queue<int> = !Queue.Init("tmp")
q.Enqueue(42)
!q.TryDequeue()
q.Length

//-------------------------------------------------------------------

let rs : ResultCell<int> = !ResultCell.Init("tmp")

async { do! Async.Sleep 10000 
        do! rs.SetResult(42) }
|> Async.Start

!rs.TryGetResult()

!rs.AwaitResult()

let ra : ResultAggregator<int> = !ResultAggregator.Init("tmp", 10)
for x in 0..9 do
    printfn "%b" <| !ra.SetResult(x, x * 10)
ra.Complete

let x = !ra.ToArray()

//-------------------------------------------------------------------
type DCTS = DistributedCancellationTokenSource

let dcts0 = !DCTS.Init("tmp")
let ct0 = dcts0.GetLocalCancellationToken()

let t1 = async { while true do 
                    do! Async.Sleep 2000
                    printfn "t1" }

Async.Start(t1, ct0)
dcts0.Cancel()

let root = !DCTS.Init("tmp")
let chain = Seq.fold (fun dcts _ -> let d = !DCTS.Init("tmp", dcts) in ignore(d.GetLocalCancellationToken()) ; d ) root {1..10}

Async.Start(t1, chain.GetLocalCancellationToken())
root.Cancel()
chain.IsCancellationRequested


//--------------------------------------------------------------------
let exp = AssemblyExporter.Init("tmp")
type Foo = Foo
let xs = exp.ComputeDependencies Foo
!exp.UploadDependencies(xs)
!exp.LoadDependencies(xs)

