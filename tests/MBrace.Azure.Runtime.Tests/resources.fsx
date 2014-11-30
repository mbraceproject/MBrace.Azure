#I "../../bin/"
#r "MBrace.Core.dll"
#r "MBrace.Library.dll"
#r "FsPickler.dll"
#r "Vagrant.dll"
#r "Microsoft.WindowsAzure.Storage.dll"
#r "Microsoft.ServiceBus.dll"
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
        StorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=krontogiannismbrace;AccountKey=Zoo2JcIuFpwPIg2XQnbDsqXgN7fFKNkI8Glmwy80cqcSzVcNyPc25qmARDfZ2RElJyTnztAqHfccw5gScAjqyg==;"
        ServiceBusConnectionString = "Endpoint=sb://krontogiannis-mbrace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=eCfW0KoChGPrL+FNL1gnf4dEyz0lWomWTV6umWY0bII="  }


Configuration.Activate(config)
Configuration.DeleteConfigurationResources(config)
open Nessos.MBrace.Azure.Runtime.Common
open Nessos.MBrace.Azure.Runtime.Resources
let (!) (task : Async<'T>) = Async.RunSynchronously task

let del x =
    ClientProvider.TableClient.ListTables(x)
    |> Seq.map (fun t -> t.DeleteAsync() |> Async.AwaitIAsyncResult)
    |> Async.Parallel
    |> Async.RunSynchronously
del "process"

ClientProvider.BlobClient.ListContainers("process")
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

let c = !BlobCell.Init(config.ConfigurationId, "tmp", fun () -> 42)

!c.GetValue()

let c' = Configuration.Serializer.Pickle(c)
let c' = Configuration.Serializer.UnPickle<BlobCell<int>>(c')
!c'.GetValue()

//-------------------------------------------------------------------

let q : Queue<int> = !Queue.Init(config.ConfigurationId, "foobar")

!(async {
    for i = 0 to 100 do
        printfn "%d" i
        do! q.Enqueue(42)
})

!q.EnqueueBatch([|0..100|])

let m = !q.TryDequeue()

!m.Value.GetPayloadAsync()
!m.Value.CompleteAsync()

q.Length

let b = Configuration.Serializer.Pickle(q)
let b' = Configuration.Serializer.UnPickle<Queue<int>>(b)
!b'.Enqueue(12)

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
let exp = AssemblyManager.Init("tmp")
type Fo = Fo
let xs = exp.ComputeDependencies Fo
!exp.UploadDependencies(xs)
!exp.LoadDependencies(xs)

//---------------------------------------------------------------------

let rf = ResourceFactory.Init(config)
let pmon = rf.ProcessMonitor
!pmon.CreateRecord("foo", "bar", "")
!pmon.GetProcesses()
!pmon.GetProcess("foo")
!pmon.SetCompleted("foo", "")

//---------------------------------------------------------------------

let t = !Topic.Init(config.ConfigurationId, config.DefaultTopic)

let s1 = "worker_" + System.Guid.NewGuid().ToString("N")
let s2 = "worker_" + System.Guid.NewGuid().ToString("N")

!t.Enqueue(42, s1)
!t.Enqueue(43, s2)

let c1 = t.GetSubscription(s1)
let c2 = t.GetSubscription(s2)

let m1 = !c1.TryDequeue()
let m2 = !c2.TryDequeue()

!m1.Value.CompleteAsync()
!m2.Value.CompleteAsync()
