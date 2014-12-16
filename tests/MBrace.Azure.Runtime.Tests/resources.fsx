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

Configuration.Activate(config) |> Async.RunSynchronously
Configuration.DeleteResources(config)


open Nessos.MBrace.Azure.Runtime.Common
open Nessos.MBrace.Azure.Runtime.Resources
let (!) (task : Async<'T>) = Async.RunSynchronously task


let factory = ChannelProvider.Create(config.ConfigurationId)

let sp, rp = !factory.CreateChannel<int>()

!sp.Send(42)

!rp.Receive()

type Foo = Foo of int

let sp, rp = !factory.CreateChannel<byte []>()

!sp.Send(Array.zeroCreate 200)
let r = !rp.Receive()
r.Length


let del x =
    ClientProvider.TableClient.ListTables(x)
    |> Seq.map (fun t -> t.DeleteAsync() |> Async.AwaitIAsyncResult)
    |> Async.Parallel
    |> Async.RunSync
del "process"

ClientProvider.BlobClient.ListContainers("process")
|> Seq.map (fun t -> t.DeleteAsync() |> Async.AwaitIAsyncResult)
|> Async.Parallel
|> Async.RunSync

//-------------------------------------------------------------------

let c = !Counter.Init("tmp", 1)
!c.Increment()

let l = !Latch.Init("tmp", 11)
!l.Decrement()

[|1..5|]
|> Array.map (fun _ -> async { do! l.Decrement() |> Async.Ignore })
|> Async.Parallel
|> Async.Ignore
|> Async.RunSync

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

let dcts0 = !(DCTS.Create(config.ConfigurationId, "tmp"))

let ct0 = dcts0.GetLocalCancellationToken()

let t1 = async { while true do 
                    do! Async.Sleep 2000
                    printfn "t1" }

Async.Start(t1, ct0)
dcts0.Cancel()

let root = !(DCTS.Create(config.ConfigurationId, "tmp"))
let chain = Seq.fold (fun dcts _ -> let d = !(DCTS.Create(config.ConfigurationId, "tmp", dcts)) in ignore(d.GetLocalCancellationToken()) ; d ) root {1..10}

Async.Start(t1, chain.GetLocalCancellationToken())
root.Cancel()
chain.IsCancellationRequested

let root = !(DCTS.Create(config.ConfigurationId, "tmp"))
let rec foo cts i n = 
    async {
        if i = n then return cts
        else 
            let! c1 = DCTS.Create(config.ConfigurationId, "tmp", cts)
            let! c2 = DCTS.Create(config.ConfigurationId, "tmp", cts)
            let! c1' = foo c1 (i+1) n
            let! c2' = foo c2 (i+1) n
            return c2'
    }

let tok = !(foo root 0 5)
root.Cancel()

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
