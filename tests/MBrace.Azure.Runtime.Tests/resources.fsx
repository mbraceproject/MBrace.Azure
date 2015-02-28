#I "../../bin/"
#r "MBrace.Core.dll"
#r "FsPickler.dll"
#r "Vagabond.dll"
#r "Microsoft.ServiceBus.dll"
#r "MBrace.Azure.Runtime.Common.dll"
#r "MBrace.Azure.Runtime.dll"
#r "MBrace.Azure.Client.dll"
#time "on"

open MBrace
open MBrace.Continuation
open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common
open MBrace.Azure.Client
open System
open System.IO

let selectEnv name =
    (Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.User),
        Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.Machine))
    |> function | null, s | s, null | s, _ -> s

let config = 
    { Configuration.Default with
        StorageConnectionString = selectEnv "azurestorageconn"
        ServiceBusConnectionString = selectEnv "azureservicebusconn" }.WithAppendedId


open MBrace.Azure.Runtime.Common
open MBrace.Azure.Runtime.Resources
let run (task : Async<'T>) = Async.RunSynchronously task
Configuration.ActivateAsync(config) |> run
let clone x = Configuration.Pickler.Clone(x)

//#region TaskQueue 

let tq = TaskQueue.Create(config.ConfigurationId, "tempqueue", "temptopic") |> run

let affinity = Guid.NewGuid().ToString "N"
tq.Affinity <- affinity

tq.EnqueueBatch(Array.init 100 string) |> run

async {
    let msgCount = ref 0
    while !msgCount < 100 do
        let! msg = tq.TryDequeue() 
        match msg with 
        | None -> ()
        | Some msg -> 
            let! payload = msg.GetPayloadAsync<string>()
            printfn "Message %s" payload
            do! msg.CompleteAsync()
            incr msgCount 
} |> run

//#endregion ----------------------

//#region Channels
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

//#endregion

//#region Cells
let c = Counter.Create(config.ConfigurationId, 1, "tmp") |> run |> clone
c.Increment() |> run

let l = Latch.Create(config.ConfigurationId, 11, "tmp") |> run |> clone
l.Decrement() |> run

[|1..5|]
|> Array.map (fun _ -> async { do! l.Decrement() |> Async.Ignore })
|> Async.Parallel
|> Async.Ignore
|> Async.RunSync

l.Value

let c = Blob.Create(config.ConfigurationId, fun () -> 42) |> run |> clone
c.GetValue() |> run

//#endregion

// #region Queue
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

//#endregion

//#region Results
let rs = ResultCell<int>.Create(config.ConfigurationId, "temp") |> run |> clone
rs.Path
async { do! Async.Sleep 10000 
        do! rs.SetResult(Result.Completed 42) }
|> Async.Start

rs.TryGetResult() |> run
rs.AwaitResult()  |> run

let ra = ResultAggregator<int>.Create(config.ConfigurationId, 10, "process000") |> run |> clone
for x in 0..9 do
    printfn "%b" <| (run <| ra.SetResult(x, x * 10))
ra.Complete

let x = ra.ToArray() |> run

//#endregion 

type DCTS = DistributedCancellationTokenSource

let dcts0 = DCTS.Create(config.ConfigurationId, "tmp") |> run

dcts0.IsCancellationRequested

let t1 = async { while true do 
                    do! Async.Sleep 2000
                    printfn "t1" }

Async.Start(t1, (dcts0 :> ICloudCancellationToken).LocalToken)
dcts0.Cancel()

let a = DCTS.Create(config.ConfigurationId, "foo") |> run
let b = DCTS.Create(config.ConfigurationId, "bar", parent = a) |> run
let c = DCTS.Create(config.ConfigurationId, "zar", parent = a) |> run
a.Links
a.Cancel()

let root = DCTS.Create(config.ConfigurationId, "tmp") |> run
let chain = 
    {1..10}
    |> Seq.fold (fun dcts _ -> 
            let d = DCTS.Create(config.ConfigurationId, "tmp", parent = dcts) |> run
            //(d :> ICloudCancellationToken).LocalToken |> ignore
            d ) root 


Async.Start(t1, (chain :> ICloudCancellationToken).LocalToken)
root.Cancel()
chain.IsCancellationRequested

let root = DCTS.Create(config.ConfigurationId, "tmp") |> run
let rec foo (cts : DCTS) i n = 
    async {
        if i = n then return cts
        else 
            let! c1 = DCTS.Create(config.ConfigurationId, "tmp", parent = cts)
            let! c2 = DCTS.Create(config.ConfigurationId, "tmp", parent = cts)
            let! c1' = foo c1 (i+1) n
            let! c2' = foo c2 (i+1) n
            return c2'
    }

let tok = foo root 0 5 |> run
root.Cancel()

tok.IsCancellationRequested

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
