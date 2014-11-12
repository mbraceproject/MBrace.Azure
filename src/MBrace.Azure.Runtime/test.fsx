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

let conn = System.IO.File.ReadAllLines "/mbrace/conn.txt"
let config = 
    { StorageConnectionString = conn.[0]
      ServiceBusConnectionString = conn.[1] }
ClientProvider.Activate config

let (!) (task : Async<'T>) = Async.RunSynchronously task

ClientProvider.TableClient.GetTableReference("tmp").Delete()
ClientProvider.BlobClient.GetContainerReference("tmp").Delete()

open Counters

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

open Cells 

let c = !BlobCell.Init(BlobCell.GetUri "tmp", fun () -> 42)
!c.GetValue<int>()

open Queues 

let q = !Queue.Init<int>(Queue.GetUri "tmp")
!q.Enqueue(42)
!q.TryDequeue()
q.Length

open Resources 

let rs  = !ResultCell.Init(ResultCell.GetUri "tmp")
async { do! Async.Sleep 10000 
        do! rs.SetResult(42) }
|> Async.Start
!rs.AwaitResult()

let ra = !ResultAggregator.Init(ResultAggregator.GetUri("tmp"), 10)
for x in 0..9 do
    printfn "%b" <| !ra.SetResult(x, x * 10)
ra.Complete

let x = !ra.ToArray()


//MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.Runtime.Azure.exe"
//
//let runtime = MBraceRuntime.InitLocal(4)
//
//let getWordCount inputSize =
//    let map (text : string) = cloud { return text.Split(' ').Length }
//    let reduce i i' = cloud { return i + i' }
//    let inputs = Array.init inputSize (fun i -> "lorem ipsum dolor sit amet")
//    MapReduce.mapReduce map 0 reduce inputs
//
//
//let t = runtime.RunAsTask(getWordCount 2000)
//do System.Threading.Thread.Sleep 3000
//runtime.KillAllWorkers() 
//runtime.AppendWorkers 4
//
//t.Result