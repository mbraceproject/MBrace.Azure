#I "../../bin/"
#r "MBrace.Core.dll"
#r "MBrace.Runtime.dll"
#r "FsPickler.dll"
#r "Vagabond.dll"
#r "MBrace.Azure.dll"
#r "MBrace.Azure.Management.dll"

open System.IO
open MBrace.Azure.Management

#time "on"

/// gets the local Cloud Service package build
let getLocalCspkg () =
    let path = __SOURCE_DIRECTORY__ + "/../../bin/cspkg/app.publish/MBrace.Azure.CloudService.cspkg" |> Path.GetFullPath
    if not <| File.Exists path then failwith "Right click on the 'MBrace.Azure.CloudService' project and hit 'Package...'."
    path

let pubSettings = PublishSettings.ParseFile "/Users/eirik/Desktop/eirik.publishSettings"
let subscription = pubSettings.GetSubscriptionById "Nessos Information Technologies SA"
let manager = SubscriptionManager.Create(subscription, Region.West_Europe, logger = ConsoleLogger())

manager.ShowDeployments()

let deployment = manager.Provision(serviceName = "eiriktest", vmCount = 4, cloudServicePackage = getLocalCspkg(), reuseAccounts = false) // deploy from local cspkg
//let deployment = manager.Provision(serviceName = "eiriktest", vmCount = 4, vmSize = VMSize.A3) // deploy from github
//let deployment = manager.GetDeployment(serviceName = "eiriktest") // fetch an already existing deployment

deployment.ShowInfo()
deployment.ShowInstanceInfo()

deployment.AwaitProvision()

deployment.Resize(vmCount = 2) // resizes the vm count

deployment.Delete(deleteStorageAccount = true, deleteServiceBusAccount = true) // *deletes* the Azure deployment

manager.Storage.ShowAccounts() // show storage accounts

// Completed, now test the cluster using MBrace

open MBrace.Azure
open MBrace.Core

let cluster = AzureCluster.Connect(deployment, logger = ConsoleLogger())

cluster.ShowWorkers()
cluster.Run(Cloud.ParallelEverywhere Cloud.CurrentWorker)
cluster.ShowProcesses()