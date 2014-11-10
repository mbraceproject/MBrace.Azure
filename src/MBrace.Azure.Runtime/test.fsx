#I "../../bin/"

#r "MBrace.Core.dll"
#r "MBrace.Library.dll"
#r "MBrace.Azure.Runtime.exe"

#r "Microsoft.WindowsAzure.Storage.dll"
#r "Microsoft.ServiceBus.dll"

open Nessos.MBrace
open Nessos.MBrace.Library
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Common

let conn = System.IO.File.ReadAllLines "/mbrace/conn.txt"

let config = 
    { StorageConnectionString = conn.[0]
      ServiceBusConnectionString = conn.[1] } 

let cp = new ClientProvider(config)

let path = { Container = "tmp"; Id = "latch0" }

let l = Latch.Init(cp, path, 0)
let l = Latch.Get(cp, path)

[|1..100|]
|> Array.map (fun _ -> async { l.Increment() })
|> Async.Parallel
|> Async.Ignore
|> Async.RunSynchronously

l.Value



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