#I "../../bin/"
#r "MBrace.Core.dll"
#r "MBrace.Library.dll"
#r "MBrace.Azure.Runtime.exe"
#r "Microsoft.WindowsAzure.Storage.dll"
#r "Microsoft.ServiceBus.dll"
#time "on"

open Nessos.MBrace
open Nessos.MBrace.Library
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Common
open Nessos.MBrace.Azure.Runtime.Resources

let conn = System.IO.File.ReadAllLines "/mbrace/conn.txt"
let config = 
    { StorageConnectionString = conn.[0]
      ServiceBusConnectionString = conn.[1] }
ClientProvider.Activate config

let (!) (task : Async<'T>) = Async.RunSynchronously task

ClientProvider.TableClient.GetTableReference("tmp").DeleteIfExists()
ClientProvider.BlobClient.GetContainerReference("tmp").DeleteIfExists()

//-------------------------------------------------------------------

let c = !Counter.Init(Counter.GetUri "tmp", 1)
!c.Increment()

let l = !Latch.Init(Latch.GetUri "tmp", 11)
!l.Decrement()

[|1..5|]
|> Array.map (fun _ -> async { do! l.Decrement() |> Async.Ignore })
|> Async.Parallel
|> Async.Ignore
|> Async.RunSynchronously

l.Value

//-------------------------------------------------------------------

let c = !BlobCell.Init(BlobCell.GetUri "tmp", fun () -> 42)
!c.GetValue()



//-------------------------------------------------------------------

let q = !Queue.Init<int>(Queue.GetUri "tmp")
q.Enqueue(42)
!q.TryDequeue()
q.Length

//-------------------------------------------------------------------

let rs : ResultCell<int> = !ResultCell.Init(ResultCell.GetUri "tmp")

async { do! Async.Sleep 10000 
        do! rs.SetResult(42) }
|> Async.Start

!rs.TryGetResult()

!rs.AwaitResult()

let ra = !ResultAggregator.Init(ResultAggregator.GetUri "tmp", 10)
for x in 0..9 do
    printfn "%b" <| !ra.SetResult(x, x * 10)
ra.Complete

let x = !ra.ToArray()

//-------------------------------------------------------------------
type DCTS = DistributedCancellationTokenSource

let dcts0 = !DCTS.Init(DCTS.GetUri "tmp")
let ct0 = dcts0.GetLocalCancellationToken()

let t1 = async { while true do 
                    do! Async.Sleep 2000
                    printfn "t1" }

Async.Start(t1, ct0)
dcts0.Cancel()

let root = !DCTS.Init(DCTS.GetUri "tmp")
let chain = Seq.fold (fun dcts _ -> let d = !DCTS.Init(DCTS.GetUri "tmp", dcts) in d.GetLocalCancellationToken() ; d ) root {1..10}

Async.Start(t1, chain.GetLocalCancellationToken())
root.Cancel()
chain.IsCancellationRequested


MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.Azure.Runtime.exe"
Config.initRuntimeState()
let runtime = MBraceRuntime.InitLocal(3)

let getWordCount inputSize =
    let map (text : string) = cloud { return text.Split(' ').Length }
    let reduce i i' = cloud { return i + i' }
    let inputs = Array.init inputSize (fun i -> "lorem ipsum dolor sit amet")
    MapReduce.mapReduce map 0 reduce inputs


//let t = runtime.RunAsTask(getWordCount 2000)
//let t = runtime.Run(cloud { return 42 })

let f x i = Cloud.Parallel  <| List.init i (fun x -> cloud { return x + i })

let t = runtime.Run(f 0 30)

runtime.Run(cloud { return 42 })

do System.Threading.Thread.Sleep 3000
runtime.KillAllWorkers() 
runtime.AppendWorkers 4

