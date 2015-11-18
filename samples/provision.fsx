#I "../bin/"
#r "MBrace.Core.dll"
#r "MBrace.Runtime.dll"
#r "FsPickler.dll"
#r "Vagabond.dll"
#r "MBrace.Azure.dll"
#r "MBrace.Azure.Management.dll"

open System.IO
open MBrace.Azure.Management

//
// This sample demonstrates how the MBrace.Azure.Management library can be used
// to deploy MBrace clusters using Azure worker roles.
//

// See https://msdn.microsoft.com/en-us/library/dn385850(v=nav.70).aspx on how
// you can retrieve your account's publishsettings file
let publishSettingsFile = "Replace with path to your local .publishsettings file"

// Set the prefered subscription name or id from your publish settings
let subscriptionName = "Azure Free Trial"

/// Set your default region
let defaultRegion = Region.West_Europe

/// gets the local Cloud Service package build; Please use visual studio to create the CloudService package before calling
let getLocalCspkg () : string =
    let path = __SOURCE_DIRECTORY__ + "/../../bin/cspkg/app.publish/MBrace.Azure.CloudService.cspkg" |> Path.GetFullPath
    if not <| File.Exists path then failwith "Right click on the 'MBrace.Azure.CloudService' project and hit 'Package...'."
    path

// Instantiate a deployment manager instance
let publishSettings = PublishSettings.ParseFile publishSettingsFile
let subscription = publishSettings.GetSubscriptionById subscriptionName
let manager = SubscriptionManager.Create(subscription, defaultRegion, logger = new ConsoleLogger())

// Get info on the current MBrace deployments
manager.ShowDeployments()

// Begin deploying your local .cspkg to a new cloud service
// VM size can be changed by updating the worker role settings and rebuilding the solution
let deployment = manager.Provision(vmCount = 4, cloudServicePackage = getLocalCspkg())

// track the deployment status
deployment.ShowInfo()

// track deployment instance status
deployment.ShowInstanceInfo()

// resize the deployment vms
deployment.Resize(vmCount = 6)

// *deletes* the deployed cluster
deployment.Delete() 

//
//  Testing the deployed MBrace cluster
//

open MBrace.Azure
open MBrace.Core

// connect to the deployment by fetching the cluster handle
let cluster = AzureCluster.Connect(deployment, logger = ConsoleLogger())

cluster.ShowWorkers()
cluster.Run(Cloud.ParallelEverywhere Cloud.CurrentWorker)
cluster.ShowProcesses()