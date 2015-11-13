namespace MBrace.Azure.Management

open System
open System.Collections.Generic
open System.IO
open System.Text.RegularExpressions
open System.Xml.Linq

open Microsoft.Azure
open Microsoft.WindowsAzure.Management
open Microsoft.WindowsAzure.Management.Models
open Microsoft.WindowsAzure.Management.Compute
open Microsoft.WindowsAzure.Management.Compute.Models

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils.PrettyPrinters
open MBrace.Azure
open MBrace.Azure.Runtime

/// Represents an Azure VM instance
type VMInstance = 
    { 
        /// Role instance identifier
        Id : string
        /// VM IP Address
        IPAddress : string
        /// VM size idenfier
        VMSize : VMSize 
        /// Deployment status for individual node
        Status : string 
    }

/// Cloud Service Deployment State
type DeploymentState =
    | NoDeployment
    | Provisioning of percentage:float
    | Ready
    | RunningTransitioning
    | SuspendedTransitioning
    | Suspending
    | Suspended
    | Deleting
    | Unknown

    override s.ToString() =
        match s with
        | NoDeployment -> "None"
        | Provisioning pct -> sprintf "Provisioning (%2.1f%% complete)" (pct * 100.)
        | Ready -> "Ready"
        | RunningTransitioning -> "RunningTransitioning"
        | SuspendedTransitioning -> "SuspendedTransitioning"
        | Suspending -> "Suspending"
        | Suspended -> "Suspended"
        | Deleting -> "Deleting"
        | Unknown -> "Unknown"

module internal Compute =

    type DeploymentDetails =
        {
            Name : string
            CreatedTime : DateTime
            ServiceStatus : string
            DeploymentState : DeploymentState
            Configuration : Configuration
            Nodes : VMInstance [] 
        }

    type DeploymentReporter private () =
        static let template : Field<DeploymentDetails> list =
            [ 
                Field.create "Name" Left (fun d -> d.Name)
                Field.create "VM size" Left (fun d -> match d.Nodes with [||] -> "N/A" | ns -> ns.[0].VMSize.Id)
                Field.create "#Instances" Left (fun d -> if Array.isEmpty d.Nodes then "N/A" else string d.Nodes.Length)
                Field.create "Created Time" Left (fun d -> d.CreatedTime) 
                Field.create "Service Status" Left (fun d -> d.ServiceStatus) 
                Field.create "Deployment Status" Left (fun d -> d.DeploymentState)
            ]

        static member Report(deployments : DeploymentDetails list, ?title : string) =
            Record.PrettyPrint(template, deployments, ?title = title, useBorders = false)

    type InstanceReporter private () =
        static let template : Field<VMInstance> list =
            [
                Field.create "Instance Id" Left (fun n -> n.Id)
                Field.create "VM Size" Left (fun n -> n.VMSize)
                Field.create "Status" Right (fun n -> n.Status)
                Field.create "IP Address" Left (fun n -> n.IPAddress)
            ]

        static member Report(nodes : VMInstance list, ?title : string) =
            Record.PrettyPrint(template, nodes, ?title = title, useBorders = false)

    let getDeploymentState (statusOpt : DeploymentStatus option) (nodes : VMInstance []) =
        let maxScore = 6 * nodes.Length
        let getNodeProvisionScore (node : VMInstance) =
            match node.Status with
            | "StoppedVM"           -> 1
            | "CreatingVM"          -> 2
            | "StartingVM"          -> 3
            | "RoleStateUnknown"    -> 4
            | "BusyRole"            -> 5
            | "ReadyRole"           -> 6
            | _                     -> -1

        match statusOpt with
        | None -> NoDeployment
        | Some status ->
            match status with
            | DeploymentStatus.Suspended -> Suspended
            | DeploymentStatus.Suspending -> Suspending
            | DeploymentStatus.RunningTransitioning -> RunningTransitioning
            | DeploymentStatus.SuspendedTransitioning -> SuspendedTransitioning
            | DeploymentStatus.Starting | DeploymentStatus.Deploying -> Provisioning 0.
            | DeploymentStatus.Deleting -> Deleting
            | DeploymentStatus.Running ->
                if Array.isEmpty nodes then Ready else

                let scores = nodes |> Array.map getNodeProvisionScore
                if scores |> Array.exists (fun s -> s < 0) then RunningTransitioning else
                let totalScore = Array.sum scores
                if totalScore = maxScore then Ready
                else
                    Provisioning(float totalScore / float maxScore)

            | _ -> Unknown

    let validateServiceName (client:SubscriptionClient) serviceName = async { 
        let! (result : HostedServiceCheckNameAvailabilityResponse) = client.Compute.HostedServices.CheckNameAvailabilityAsync serviceName
        if not result.IsAvailable then return invalidOp result.Reason
    }

    let tryGetDeploymentConfiguration (serviceName : string) (client : SubscriptionClient) = async {
        let! result = client.Compute.HostedServices.GetDetailedAsync serviceName |> Async.AwaitTaskCorrect |> Async.Catch
        match result with
        | Choice1Of2 service when service.Properties.ExtendedProperties |> Common.isMBraceAsset ->
            let storageConnectionString = service.Properties.ExtendedProperties.["StorageConnectionString"]
            let serviceBusConnectionString = service.Properties.ExtendedProperties.["ServiceBusConnectionString"]
            let config = new Configuration(storageConnectionString, serviceBusConnectionString)
            return Some config
        | _ ->
            return None
    }

    let tryGetDeploymentInfo (client:SubscriptionClient) (getProps : Async<HostedServiceProperties>) (serviceName:string) = async {
        let! deploymentT = 
            async {
                let dplmnts = client.Compute.Deployments
                return! dplmnts.GetBySlotAsync(serviceName, DeploymentSlot.Production)
            } |> Async.Catch |> Async.StartChild

        let! properties = getProps

        if properties.ExtendedProperties |> Common.isMBraceAsset then
            let! deployment = deploymentT
            let config =
                let storageConnectionString = properties.ExtendedProperties.["StorageConnectionString"]
                let serviceBusConnectionString = properties.ExtendedProperties.["ServiceBusConnectionString"]
                new Configuration(storageConnectionString, serviceBusConnectionString)

            let nodes =
                match deployment with
                | Choice2Of2 _ -> [||]
                | Choice1Of2 d -> 
                    d.RoleInstances 
                    |> Seq.map (fun i -> { Id = i.InstanceName ; IPAddress = i.IPAddress.ToString() ; VMSize = VMSize.Define i.InstanceSize ; Status = i.InstanceStatus }) 
                    |> Seq.toArray

            let state = getDeploymentState (match deployment with Choice1Of2 d -> Some d.Status | _ -> None) nodes

            let info = 
                {  
                    Name = serviceName
                    CreatedTime = properties.DateCreated
                    ServiceStatus = string properties.Status
                    DeploymentState = state
                    Configuration = config
                    Nodes = nodes 
                }

            return Some info
        else
            return None
    }

    let tryGetRunningDeployment (client:SubscriptionClient) (serviceName:string) = async {
        let getProperties () = async {
            let! (service : HostedServiceGetDetailedResponse) = client.Compute.HostedServices.GetDetailedAsync serviceName
            return service.Properties
        }

        return! tryGetDeploymentInfo client (getProperties()) serviceName
    }

    let getRunningDeployments (client:SubscriptionClient) = async {
        let! (services : HostedServiceListResponse) = client.Compute.HostedServices.ListAsync()
        let getProperties (s : HostedServiceListResponse.HostedService) = async { return s.Properties }
        let! info = services |> Seq.map (fun s -> tryGetDeploymentInfo client (getProperties s) s.ServiceName) |> Async.Parallel 
        return info |> Seq.choose id |> Seq.toList
    }

    let buildMBraceConfig serviceName instances storageConnection serviceBusConnection useDiagnostics =
        sprintf """<?xml version="1.0" encoding="utf-8"?>
<ServiceConfiguration serviceName="%s" xmlns="http://schemas.microsoft.com/ServiceHosting/2008/10/ServiceConfiguration" osFamily="4" osVersion="*" schemaVersion="2015-04.2.6">
    <Role name="MBrace.Azure.WorkerRole">
    <Instances count="%d" />
    <ConfigurationSettings>
        <Setting name="MBrace.StorageConnectionString" value="%s" />
        <Setting name="MBrace.ServiceBusConnectionString" value="%s" />
        <Setting name="Microsoft.WindowsAzure.Plugins.Diagnostics.ConnectionString" value="%s" />
    </ConfigurationSettings>
    </Role>
</ServiceConfiguration>""" serviceName instances storageConnection serviceBusConnection (if useDiagnostics then storageConnection else "")

    let prepareMBraceServiceDeployment (logger : ISystemLogger) (serviceName : string) (clusterLabel : string) 
                                        (region : Region) (packagePath : string) (config : string) 
                                        (storageAccountName : string) (storageConnectionString : string) 
                                        (serviceBusNamespace : string) (serviceBusConnectionString : string) (client:SubscriptionClient) = async {

        let extendedProperties =
            dict [
                yield! Common.defaultExtendedProperties |> Seq.map (fun kv -> kv.Key, kv.Value)
                yield ("StorageAccountName", storageAccountName)
                yield ("StorageConnectionString", storageConnectionString)
                yield ("ServiceBusName", serviceBusNamespace)
                yield ("ServiceBusConnectionString", serviceBusConnectionString)
            ]

        logger.Logf LogLevel.Info "creating cloud service %s" serviceName
        let! _ = client.Compute.HostedServices.CreateAsync(HostedServiceCreateParameters(Location = region.Id, ServiceName = serviceName, ExtendedProperties = extendedProperties))

        let! container = Storage.getDeploymentContainer storageConnectionString
        let packageBlob = packagePath |> Path.GetFileName |> container.GetBlockBlobReference
        let blobSizesDoNotMatch() =
            packageBlob.FetchAttributes()
            packageBlob.Properties.Length <> FileInfo(packagePath).Length

        if (not (packageBlob.Exists()) || blobSizesDoNotMatch()) then
            logger.Logf LogLevel.Info "uploading package %A" packagePath
            do! packageBlob.UploadFromFileAsync(packagePath, FileMode.Open)
        
        logger.Logf LogLevel.Info "scheduling cluster creation:\n  cluster %s\n  package uri %s\n  config %s" serviceName (packageBlob.Uri.ToString()) config
        return DeploymentCreateParameters(
            Label = clusterLabel,
            Name = serviceName,
            PackageUri = packageBlob.Uri,
            Configuration = config,
            StartDeployment = Nullable true,
            TreatWarningsAsError = Nullable true)
        }

    let beginDeploy (useStaging : bool) (deployParams : DeploymentCreateParameters) (client : SubscriptionClient) = async {
        let slot = if useStaging then DeploymentSlot.Staging else DeploymentSlot.Production
        let! (createOp : AzureOperationResponse) = client.Compute.Deployments.BeginCreatingAsync(deployParams.Name, slot, deployParams)
        if createOp.StatusCode <> Net.HttpStatusCode.Accepted then 
            return failwithf "error: HTTP request for creation operation %A was not accepted (status code: %O)" deployParams.Name createOp.StatusCode
    }  

    let deleteMBraceDeployment (logger : ISystemLogger) (serviceName:string) (client:SubscriptionClient) = async {
        let! (service : HostedServiceGetDetailedResponse) = client.Compute.HostedServices.GetDetailedAsync serviceName
        if service.Properties.ExtendedProperties |> Common.isMBraceAsset then
            logger.Logf LogLevel.Info "deleting cluster %s" serviceName
            let! result = client.Compute.Deployments.DeleteByNameAsync(serviceName, serviceName, true) |> Async.AwaitTaskCorrect |> Async.Catch
            match result with
            | Choice1Of2 deleteOp when deleteOp.Status = OperationStatus.Succeeded -> ()
            | Choice1Of2 deleteOp -> return failwithf "Failed to delete deployment %A: %s" serviceName deleteOp.Error.Message
            | Choice2Of2 _ -> logger.Logf LogLevel.Warning "No deployment for cloud service %A could be found." serviceName

            let! (deleteOp : AzureOperationResponse) = client.Compute.HostedServices.DeleteAsync serviceName
            if deleteOp.StatusCode <> Net.HttpStatusCode.OK then return failwith (deleteOp.StatusCode.ToString()) 
        else
            logger.Logf LogLevel.Info "No MBrace cluster called %A found" serviceName
    }

    let downloadServicePackage (logger : ISystemLogger) (vmSize : VMSize) (mbraceVersion : string option) (uri : string option) = async {
        let uri, version =
            match uri with
            | Some u -> Uri u, None
            | None ->
                let mbraceVersion = defaultArg mbraceVersion Common.defaultMBraceVersion
                Common.getPackageUrl mbraceVersion vmSize |> Uri, Some mbraceVersion

        if uri.IsFile then
            logger.Logf LogLevel.Info "using cloud service package from %A" uri.LocalPath 
            return uri.LocalPath, version
        else
            use wc = new System.Net.WebClient()
            let tmp = System.IO.Path.GetTempFileName()
            logger.Logf LogLevel.Info "downloading cloud service package from %A" uri
            do! wc.DownloadFileTaskAsync(uri, tmp)
            return tmp, version
    }

module internal Infrastructure =

    let private requiredServices = [| "Compute" ; "Storage" |]

    /// fetches a list of all regions together with supported vm sizes
    let listRegions (client:SubscriptionClient) = async {
        let! listedRoleSizesT = client.Management.RoleSizes.ListAsync() |> Async.AwaitTaskCorrect |> Async.StartChild
        let! locations = client.Management.Locations.ListAsync() |> Async.AwaitTaskCorrect
        let! listedRoleSizes = listedRoleSizesT
        let rolesForClient = listedRoleSizes |> Seq.map (fun r -> r.Name) |> set
        return 
            locations
            |> Seq.filter(fun l -> requiredServices |> Array.forall l.AvailableServices.Contains)
            |> Seq.map(fun l -> l.Name, l.ComputeCapabilities.WebWorkerRoleSizes |> Seq.filter rolesForClient.Contains |> Seq.toArray)
            |> Seq.toArray
    }

    /// check whether region and vmsize combination is compatible
    let checkCompatibility (region : Region) (vmsize : VMSize) (client:SubscriptionClient) = async {
        let! regions = listRegions client
        let isCompatible = regions |> Array.exists (fun (location, sizes) -> location = region.Id && sizes |> Array.exists ((=) vmsize.Id))
        do if not isCompatible then failwithf "Region %A does not support VM size %A" region.Id vmsize.Id
    }