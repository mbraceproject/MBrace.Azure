
#load "helpers.fsx"

open Nessos.MBrace
open Nessos.MBrace.Azure
open Nessos.MBrace.Azure.Client
open Nessos.MBrace.Azure.Runtime


let config = 
    { Configuration.Default with
        StorageConnectionString = Helpers.azureStorageConn
        ServiceBusConnectionString = Helpers.serviceBusConn }


let runtime = Runtime.GetHandle(config)


runtime.ShowWorkers()
runtime.ShowLogs()

runtime.Run(cloud { return "Hello world"})



open Nessos.Streams
open Nessos.MBrace.Streams

let xs = 
    [|1..10|]
    |> CloudStream.ofArray
    |> CloudStream.map (fun x -> x * x)
    |> CloudStream.toArray


runtime.Run xs

let ps1 = runtime.CreateProcess(CloudFile.Enumerate "wikipedia", name = "enumeratefiles")
let files = ps1.AwaitResult()

let getTop files count =
    files
    |> CloudStream.ofCloudFiles CloudFile.ReadAllText
    |> CloudStream.collect (fun text -> Helpers.splitWords text |> Stream.ofArray |> Stream.map Helpers.wordTransform)
    |> CloudStream.filter Helpers.wordFilter
    |> CloudStream.countBy id
    |> CloudStream.sortBy (fun (_,c) -> -c) count
    |> CloudStream.toArray

let ps = runtime.CreateProcess(getTop files 20, name = "getTop with cloudfiles")
let r = ps.AwaitResult()


let mkCloudArray files =
    cloud {
        let! cloudarray =
            files
            |> CloudStream.ofCloudFiles CloudFile.ReadAllText
            |> CloudStream.collect (fun text -> Helpers.splitWords text |> Stream.ofArray |> Stream.map Helpers.wordTransform)
            |> CloudStream.filter Helpers.wordFilter
            |> CloudStream.toCloudArray
        return! CloudStream.cache cloudarray
    }

let getTop' cloudarray count =
    cloudarray
    |> CloudStream.ofCloudArray
    |> CloudStream.countBy id
    |> CloudStream.sortBy (fun (_,c) -> -c) count
    |> CloudStream.toArray

let caProcess = runtime.CreateProcess(mkCloudArray files, name = "create cloudarray")
let ca = caProcess.AwaitResult()

let ps' = runtime.CreateProcess(getTop' ca 20, name = "getTop with cloudarray")
let r' = ps'.AwaitResult()
