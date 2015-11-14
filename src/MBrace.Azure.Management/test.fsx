#I "../../bin/"
#r "MBrace.Core.dll"
#r "MBrace.Runtime.dll"
#r "FsPickler.dll"
#r "Vagabond.dll"
#r "MBrace.Azure.dll"
#r "MBrace.Azure.Management.dll"

open MBrace.Azure.Management

#time

let pubSettings = PublishSettings.ParseFile "/Users/eirik/Desktop/eirik.publishSettings"
let subscription = pubSettings.["Nessos"]
let manager = DeploymentManager.Create(subscription, Region.West_Europe, logger = ConsoleLogger())

manager.ShowDeployments()

let deployment = manager.Deploy(serviceName = "eiriktest", vmCount = 4, vmSize = VMSize.A3)
//let deployment = manager.GetDeployment(serviceName = "eiriktest")

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