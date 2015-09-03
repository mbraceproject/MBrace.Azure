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


MBraceCluster.LocalWorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.azureworker.exe"
let runtime = MBraceCluster.InitOnCurrentMachine(config, 8, 32, logger = ConsoleLogger(true), logLevel = LogLevel.Debug)
//let runtime = MBraceCluster.GetHandle(config, logger = ConsoleLogger(true), logLevel = LogLevel.Debug)
runtime.Reset(true,true,true,true,true,true,false)
runtime.KillAllLocalWorkers()
runtime.Workers

runtime.RunOnCloud(Cloud.ParallelEverywhere(Cloud.CurrentWorker))