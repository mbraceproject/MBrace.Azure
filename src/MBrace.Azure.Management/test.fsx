#I "../../bin/"
#r "MBrace.Core.dll"
#r "MBrace.Runtime.dll"
#r "FsPickler.dll"
#r "Vagabond.dll"
#r "MBrace.Azure.dll"
#r "MBrace.Azure.Management.dll"

open System.IO
open MBrace.Azure.Management

#time

/// gets the local Cloud Service package build
let getLocalCspkg () =
    let buildCfg = "Debug_AzureSDK"
    let path = __SOURCE_DIRECTORY__ + "/../../src/MBrace.Azure.CloudService/bin/" + buildCfg + "/app.publish/MBrace.Azure.CloudService.cspkg"
    if not <| File.Exists path then failwith "Right click on the 'MBrace.Azure.CloudService' project and hit 'Package...'."
    path

let pubSettings = PublishSettings.ParseFile "/Users/eirik/Desktop/eirik.publishSettings"
let subscription = pubSettings.["Nessos"]
let manager = DeploymentManager.Create(subscription, Region.West_Europe, logger = ConsoleLogger())

manager.ShowDeployments()

let deployment = manager.Deploy(serviceName = "eiriktest", vmCount = 4, cloudServicePackage = getLocalCspkg()) // deploy from local cspkg
//let deployment = manager.Deploy(serviceName = "eiriktest", vmCount = 4, vmSize = VMSize.A3) // deploy from github
//let deployment = manager.GetDeployment(serviceName = "eiriktest") // fetch an already existing deployment

deployment.ShowInfo()
deployment.ShowInstanceInfo()

deployment.AwaitProvision()

deployment.Delete() // *deletes* the Azure deployment

// Completed, now test the cluster using MBrace

open MBrace.Azure
open MBrace.Core

let cluster = AzureCluster.Connect(deployment, logger = ConsoleLogger())

cluster.ShowWorkers()
cluster.Run(cloud { return System.Environment.MachineName })
cluster.ShowProcesses()