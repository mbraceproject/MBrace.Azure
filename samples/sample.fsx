#I "../bin/"
#r "MBrace.Core.dll"
#r "MBrace.Runtime.dll"
#r "FsPickler.dll"
#r "Vagabond.dll"
#r "MBrace.Azure.dll"

open MBrace.Core
open MBrace.Azure

// gather connection string info from environment
//Configuration.EnvironmentStorageConnectionString <- "save your storage connection string"
//Configuration.EnvironmentServiceBusConnectionString <- "save your service bus connection string"
let config = Configuration.FromEnvironmentVariables()

// connect to an already provisioned MBrace cluster using supplied configuration object
let cluster = AzureCluster.Connect(config, logger = ConsoleLogger(true), logLevel = LogLevel.Debug)

// Alternatively : 
//AzureWorker.LocalExecutable <- __SOURCE_DIRECTORY__ + "/../bin/mbrace.azureworker.exe"
//let cluster = AzureCluster.InitOnCurrentMachine(config, workerCount = 4, logger = ConsoleLogger(true), logLevel = LogLevel.Debug)

// check that workers are active
cluster.ShowWorkers()

// create a sample process
let ct = cluster.CreateProcess (cloud { return System.DateTime.Now })

ct.ShowInfo()
ct.Result

// CloudFlow example

#r "MBrace.Flow.dll"
open MBrace.Flow

CloudFlow.OfHttpFileByLine "https://raw.githubusercontent.com/mbraceproject/MBrace.Azure/master/README.md"
|> CloudFlow.collect (fun line -> line.Split(' '))
|> CloudFlow.filter (fun w -> w.Length > 3)
|> CloudFlow.map (fun w -> w.ToLower())
|> CloudFlow.countBy id
|> CloudFlow.sortBy (fun (_,c) -> -c) 10
|> CloudFlow.toArray
|> cluster.Run

// reset the cluster state
cluster.Reset()