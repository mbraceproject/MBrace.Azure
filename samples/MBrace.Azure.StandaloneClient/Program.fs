// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.

open System
open System.IO
open MBrace.Core
open MBrace.Azure

[<EntryPoint>]
let main argv = 

    AzureWorker.LocalExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.azureworker.exe"

    let config = Configuration.FromEnvironmentVariables()
    let cluster = AzureCluster.Connect(config, logger = new ConsoleLogger())

    cluster.AttachLocalWorkers(workerCount = 2)

    let task = cloud { return "Hello world!" } |> cluster.Run
        
    Console.ReadLine() |> ignore
    0 // return an integer exit code