#I "../../bin/"
#r "MBrace.Core.dll"
#r "MBrace.Runtime.dll"
#r "FsPickler.dll"
#r "Vagabond.dll"
#r "MBrace.Azure.dll"
#r "MBrace.Azure.Management.dll"

open MBrace.Azure
open MBrace.Azure.Management

let pubSettings = PublishSettings.ParseFile "/Users/eirik/Desktop/eirik.publishSettings"
let subscription = pubSettings.["Nessos"]
let manager = DeploymentManager.Create(subscription, Region.West_Europe, logger = ConsoleLogger(true))

manager.ShowDeployments()
manager.DeleteDeployment "eiriktest"

let config = manager.BeginDeploy(serviceName = "eiriktest", vmCount = 2, vmSize = VMSize.A2)

//let config = manager.GetConfiguration(serviceName = "eiriktest")
//manager.DeleteDeployment "eiriktest"

let cluster = AzureCluster.Connect config

cluster.ShowWorkers()