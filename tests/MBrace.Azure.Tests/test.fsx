#I "../../bin/"
#r "MBrace.Core.dll"
#r "MBrace.Runtime.dll"
#r "FsPickler.dll"
#r "Vagabond.dll"
#r "MBrace.Azure.dll"
#time "on"

open MBrace.Core
open MBrace.Azure
open System

let config = 
    let selectEnv name = Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.User)
    new Configuration(selectEnv "azurestorageconn", selectEnv "azureservicebusconn")


AzureCluster.LocalWorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.azureworker.exe"
let cluster = AzureCluster.InitOnCurrentMachine(config, 4, 32, logger = ConsoleLogger(true), logLevel = LogLevel.Debug)
//let cluster = AzureCluster.Connect(config, logger = ConsoleLogger(true), logLevel = LogLevel.Debug)
cluster.Reset(true,true,true,true,true,true,false)
cluster.KillAllLocalWorkers()
cluster.Workers

let ct = cluster.Submit (cloud { return! Cloud.ParallelEverywhere(Cloud.CurrentWorker) })

ct.ShowInfo()
ct.Result
cluster.ShowWorkers()

let worker = cluster.Workers.[0]
worker.ShowSystemLogs()
cluster.ShowProcesses()
cluster.ClearAllProcesses()

let proc = cluster.GetProcessById "8e39011b-957f-47e7-8e16-d28313a8b7ad" :?> CloudProcess<unit>

proc.Result

//cluster.CreateTask()
let resetAzureWorker (worker : IWorkerRef) =
    try cluster.Run(cloud { exit 1 }, faultPolicy = FaultPolicy.NoRetry, target = worker, taskName = "killer Process")
    with :? FaultException -> ()

let getCurrentPid (worker : IWorkerRef) =
    cluster.Run(cloud { return System.Diagnostics.Process.GetCurrentProcess().Id }, target = worker)


getCurrentPid worker // 3424
resetAzureWorker worker
getCurrentPid worker // 1948

#r "/Users/eirik/Desktop/Google.ortools.dll"

let ass = typeof<Google.OrTools.NestedArrayHelper>.Assembly

let name = ass.GetName()


let entry = System.Reflection.Assembly.LoadFrom(@"C:\Program Files (x86)\Microsoft SDKs\F#\3.1\Framework\v4.0\fsi.exe")

let entryName = entry.GetName()

entryName.ProcessorArchitecture

Reflection.ProcessorArchitecture.Arm