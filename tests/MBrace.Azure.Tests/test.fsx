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


AzureWorker.LocalExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.azureworker.exe"
let cluster = AzureCluster.InitOnCurrentMachine(config, workerCount = 4, logger = ConsoleLogger(true), logLevel = LogLevel.Debug)
//let cluster = AzureCluster.Connect(config, logger = ConsoleLogger(true), logLevel = LogLevel.Debug)
cluster.Reset(deleteUserData = true, deleteAssemblyData = true, force = true)
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

let proc = cluster.Submit(cloud { return 42}, target = worker)

// Test fault data
cluster.Submit(
    cloud { 
        do! Cloud.Sleep 10000
        return! Cloud.TryGetFaultData()
    }, faultPolicy = FaultPolicy.WithMaxRetries 1)