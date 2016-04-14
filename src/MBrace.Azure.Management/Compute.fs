namespace MBrace.Azure.Management

open System
open System.Threading
open System.Collections.Generic
open System.IO
open System.Text.RegularExpressions
open System.Security.Cryptography
open System.Xml.Linq

open Microsoft.Azure
open Microsoft.WindowsAzure.Management
open Microsoft.WindowsAzure.Management.Models
open Microsoft.WindowsAzure.Management.Compute
open Microsoft.WindowsAzure.Management.Compute.Models

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.String
open MBrace.Runtime.Utils.PrettyPrinters
open MBrace.Azure
open MBrace.Azure.Runtime

module internal Compute =
    open Microsoft.WindowsAzure.Storage.Blob
    open System.Net

    type WADeploymentStatus = Microsoft.WindowsAzure.Management.Compute.Models.DeploymentStatus

    let private getTextHash (text : string) =
        let bytes = System.Text.Encoding.UTF8.GetBytes text
        let hash = MD5.Create().ComputeHash bytes
        Convert.BytesToBase32 hash

    let private getFileHash (path : string) =
        use fs = File.OpenRead path
        let hash = MD5.Create().ComputeHash fs
        Convert.BytesToBase32 hash

    type DeploymentReporter private () =
        static let template : Field<DeploymentInfo> list =
            [ 
                Field.create "Name" Left (fun d -> d.Name)
                Field.create "Region" Left (fun d -> d.Region)
                Field.create "VM size" Left (fun d -> match d.VMInstances with [||] -> "N/A" | ns -> ns.[0].VMSize.Id)
                Field.create "#Instances" Left (fun d -> if Array.isEmpty d.VMInstances then "N/A" else string d.VMInstances.Length)
                Field.create "Deployment Status" Left (fun d -> match d.DeploymentState with | NoDeployment | Unknown | Provisioning 0. -> d.DeploymentRequestDetails | _ -> d.DeploymentState.ToString() )
                Field.create "Storage Accnt" Left (fun d -> d.StorageAccount.AccountName)
                Field.create "ServiceBus Accnt" Left (fun d -> d.ServiceBusAccount.AccountName)
                Field.create "Last Modified" Left (fun d -> d.LastModified.LocalDateTime) 
                Field.create "Cluster Label" Left (fun d -> if String.IsNullOrEmpty d.Label then "N/A" else d.Label)
            ]

        static member Report(deployments : DeploymentInfo list, ?title : string) =
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

    let getDeploymentState (statusOpt : WADeploymentStatus option) (nodes : VMInstance []) =
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
            | WADeploymentStatus.Suspended -> Suspended
            | WADeploymentStatus.Suspending -> Suspending
            | WADeploymentStatus.RunningTransitioning -> RunningTransitioning
            | WADeploymentStatus.SuspendedTransitioning -> SuspendedTransitioning
            | WADeploymentStatus.Starting | WADeploymentStatus.Deploying -> Provisioning 0.
            | WADeploymentStatus.Deleting -> Deleting
            | WADeploymentStatus.Running ->
                if Array.isEmpty nodes then Ready else

                let scores = nodes |> Array.map getNodeProvisionScore
                if scores |> Array.exists (fun s -> s < 0) then RunningTransitioning else
                let totalScore = Array.sum scores
                if totalScore = maxScore then Ready
                else
                    Provisioning(float totalScore / float maxScore)

            | _ -> Unknown

    let validateServiceName (client:SubscriptionClient) serviceName = async { 
        let! result = client.Compute.HostedServices.CheckNameAvailabilityAsync serviceName |> Async.AwaitTaskCorrect
        if not result.IsAvailable then return invalidOp result.Reason
    }

    let getDeploymentContainer (account : StorageAccount) = async {
        let container = account.Inner.BlobClient.GetContainerReference "deployments"
        let! _result = container.CreateIfNotExistsAsync() |> Async.AwaitTaskCorrect
        return container
    }

    let tryGetDeploymentConfiguration (serviceName : string) (client : SubscriptionClient) = async {
        let! result = client.Compute.HostedServices.GetDetailedAsync serviceName |> Async.AwaitTaskCorrect |> Async.Catch
        match result with
        | Choice1Of2 service when service.Properties.ExtendedProperties |> Common.isMBraceAsset ->
            try 
                let storageConnectionString = service.Properties.ExtendedProperties.["StorageConnectionString"]
                let serviceBusConnectionString = service.Properties.ExtendedProperties.["ServiceBusConnectionString"]
                let config = new Configuration(storageConnectionString, serviceBusConnectionString)
                return Some config

            with :? KeyNotFoundException -> return None
        | _ ->
            return None
    }

    let tryGetDeploymentInfo (client:SubscriptionClient) (getProps : Async<HostedServiceProperties>) (serviceName:string) = async {
        let! deploymentT = 
            async {
                let dplmnts = client.Compute.Deployments
                return! dplmnts.GetBySlotAsync(serviceName, DeploymentSlot.Production) |> Async.AwaitTaskCorrect
            } |> Async.Catch |> Async.StartChild

        let! properties = getProps

        if properties.ExtendedProperties |> Common.isMBraceAsset then
            let! deployment = deploymentT
            let result =
                try(properties.ExtendedProperties.["StorageConnectionString"] |> StorageAccount.FromConnectionString,
                    properties.ExtendedProperties.["ServiceBusConnectionString"] |> ServiceBusAccount.FromConnectionString,
                    properties.ExtendedProperties.["DeploymentRequestId"])
                    |> Some
                with :? KeyNotFoundException -> None

            match result with
            | None -> return None
            | Some (storageAccount, serviceBusAccount, deploymentRequestId) ->

            let! lastStatus = client.Compute.GetOperationStatusAsync(deploymentRequestId) |> Async.AwaitTaskCorrect
            let nodes =
                match deployment with
                | Choice2Of2 _ -> [||]
                | Choice1Of2 d -> 
                    d.RoleInstances 
                    |> Seq.map (fun i -> { Id = i.InstanceName ; IPAddress = i.IPAddress.ToString() ; VMSize = VMSize.Define i.InstanceSize ; Status = i.InstanceStatus }) 
                    |> Seq.sortBy (fun r -> r.Id)
                    |> Seq.toArray

            let state = getDeploymentState (match deployment with Choice1Of2 d -> Some d.Status | _ -> None) nodes

            let info = 
                {  
                    Name = serviceName
                    CreatedTime = new DateTimeOffset(properties.DateCreated)
                    LastModified = new DateTimeOffset(properties.DateLastModified)
                    Label = match deployment with Choice1Of2 d -> d.Label | _ -> ""
                    ServiceStatus = string properties.Status
                    DeploymentState = state
                    StorageAccount = storageAccount
                    ServiceBusAccount = serviceBusAccount
                    VMInstances = nodes 
                    Region = Region.Define properties.Location
                    DeploymentRequestDetails =
                        let header, error = match lastStatus.Error with | null -> "Validation ", "" | error -> "", sprintf "(%s)" error.Message
                        sprintf "%s%O %s" header lastStatus.Status error
                }

            return Some info
        else
            return None
    }

    let tryGetRunningDeployment (client:SubscriptionClient) (serviceName:string) = async {
        let getProperties () = async {
            let! service = client.Compute.HostedServices.GetDetailedAsync serviceName |> Async.AwaitTaskCorrect
            return service.Properties
        }

        return! tryGetDeploymentInfo client (getProperties()) serviceName
    }

    let getRunningDeployments (client:SubscriptionClient) = async {
        let! services = client.Compute.HostedServices.ListAsync() |> Async.AwaitTaskCorrect
        let getProperties (s : HostedServiceListResponse.HostedService) = async { return s.Properties }
        let! info = services |> Seq.map (fun s -> tryGetDeploymentInfo client (getProperties s) s.ServiceName) |> Async.Parallel 
        return info |> Seq.choose id |> Seq.toList
    }

    let buildMBraceConfig serviceName instances useDiagnostics  
            (storageAccount : StorageAccount) (serviceBusAccount : ServiceBusAccount) =

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
</ServiceConfiguration>""" serviceName instances storageAccount.ConnectionString serviceBusAccount.ConnectionString (if useDiagnostics then storageAccount.ConnectionString else "")

    type PackageDetails = Official of mbraceVersion:string * VMSize | CustomRemote of Uri | CustomLocal of FileInfo
    type PackageSource = Remote of Uri * Uri | Local of FileInfo

    let deployPackage packageDetails destinationStorageAccount (logger:ISystemLogger) = async {
        let! container = getDeploymentContainer destinationStorageAccount
        let! packageSource =
            async {
                match packageDetails with
                | Official (mbraceVersion, vmSize) ->
                    // GitHub redirects to AWS for actual resources, so get the real uri of the file
                    let originalUri = Common.getPackageUrl mbraceVersion vmSize |> Uri
                    let webRequest = HttpWebRequest.CreateHttp originalUri
                    webRequest.AllowAutoRedirect <- false
                    let! response = webRequest.GetResponseAsync() |> Async.AwaitTaskCorrect
                    return Remote(originalUri, response.Headers.["Location"] |> Uri)
                | CustomRemote uri -> return Remote(uri, uri)
                | CustomLocal fileInfo -> return (Local fileInfo)
            }
        
        match packageSource with
        | Remote (namedUri, resourceUri) ->       
            let blobName =
                let uriHash = getTextHash (namedUri.ToString())
                sprintf "%s-%s" (Path.GetFileName namedUri.LocalPath) uriHash
            let packageBlob = container.GetBlockBlobReference blobName
            let! blobExists = packageBlob.ExistsAsync() |> Async.AwaitTaskCorrect
            if not blobExists then
                logger.Logf LogLevel.Info "deploying cloud service package package from %A to your storage account (%O)" namedUri packageBlob.Uri
                do! packageBlob.StartCopyFromBlobAsync resourceUri |> Async.AwaitTaskCorrect |> Async.Ignore
                while (packageBlob.CopyState.Status = CopyStatus.Pending) do
                    do! Async.Sleep 1000
                    do! packageBlob.FetchAttributesAsync() |> Async.AwaitTaskCorrect
                if packageBlob.CopyState.Status <> CopyStatus.Success then
                    return invalidOp <| sprintf "error: Unable to deploy MBrace Cloud Service package: %s" packageBlob.CopyState.StatusDescription
            return packageBlob.Uri
        | Local file -> 
            let blobName =
                let fileHash = getFileHash (file.FullName)
                sprintf "%s-%s-%d" file.Name fileHash file.Length
            let packageBlob = container.GetBlockBlobReference blobName
            let! blobExists = packageBlob.ExistsAsync() |> Async.AwaitTaskCorrect
            if not blobExists then
                logger.Logf LogLevel.Info "uploading cloud service package package from %A to your storage account (%O)" file.FullName packageBlob.Uri
                do! packageBlob.UploadFromFileAsync(file.FullName, FileMode.Open) |> Async.AwaitTaskCorrect
            return packageBlob.Uri
        }

    let createDeployment (logger : ISystemLogger) (serviceName : string) (clusterLabel : string) 
                            (region : Region) packageDetails (useStaging : bool) (enableDiagnostics : bool) (instanceCount : int)
                            (storageAccount : StorageAccount) (serviceBusAccount : ServiceBusAccount) 
                            (client:SubscriptionClient) = async {
        let! destinationPackageUri = deployPackage packageDetails storageAccount logger
        let config = buildMBraceConfig serviceName instanceCount enableDiagnostics storageAccount serviceBusAccount
        logger.Logf LogLevel.Info "creating cloud service %A" serviceName
        let! _ = client.Compute.HostedServices.CreateAsync(HostedServiceCreateParameters(Location = region.Id, ServiceName = serviceName)) |> Async.AwaitTaskCorrect
        let deployParams = 
            DeploymentCreateParameters(
                Name = serviceName,
                Label = clusterLabel,
                PackageUri = destinationPackageUri,
                Configuration = config,
                StartDeployment = Nullable true,
                TreatWarningsAsError = Nullable true)

        let slot = if useStaging then DeploymentSlot.Staging else DeploymentSlot.Production
        logger.Logf LogLevel.Info "starting deployment %A using slot %A with package %A" deployParams.Name (string slot) (string deployParams.PackageUri)
        let! createOp = client.Compute.Deployments.BeginCreatingAsync(deployParams.Name, slot, deployParams) |> Async.AwaitTaskCorrect
        let extendedProperties =
            dict [
                yield! Common.defaultExtendedProperties |> Seq.map (fun kv -> kv.Key, kv.Value)
                yield ("StorageAccountName", storageAccount.AccountName)
                yield ("StorageConnectionString", storageAccount.ConnectionString)
                yield ("ServiceBusName", serviceBusAccount.AccountName)
                yield ("ServiceBusConnectionString", serviceBusAccount.ConnectionString)
                yield ("DeploymentRequestId", createOp.RequestId)
            ]

        let! _ = client.Compute.HostedServices.UpdateAsync(serviceName, HostedServiceUpdateParameters(ExtendedProperties = extendedProperties)) |> Async.AwaitTaskCorrect
        if createOp.StatusCode <> Net.HttpStatusCode.Accepted then 
            return invalidOp <| sprintf "error: HTTP request for creation operation %A was not accepted (status code: %O)" deployParams.Name createOp.StatusCode
    }

    let resizeDeployment (logger : ISystemLogger) (serviceName : string) (newCount : int) (client : SubscriptionClient) = async {
        let! info = tryGetRunningDeployment client serviceName
        match info with
        | None -> invalidOp <| sprintf "could not find deployment under %A" serviceName
        | Some di when di.VMInstances.Length = newCount ->
            logger.Logf LogLevel.Info "deployment %A already containing %d instances, no update needed." serviceName newCount
            return ()
        | Some di ->
            let newConfiguration = buildMBraceConfig serviceName newCount true di.StorageAccount di.ServiceBusAccount
            let changeParams = new DeploymentChangeConfigurationParameters(Configuration = newConfiguration)
            let! changeOp = client.Compute.Deployments.ChangeConfigurationByNameAsync(serviceName, serviceName, changeParams) |> Async.AwaitTaskCorrect
            if changeOp.StatusCode <> Net.HttpStatusCode.OK then
                return invalidOp <| sprintf "error: HTTP request for change operation %A was not accepted (status code: %O)" serviceName changeOp.StatusCode
    }

    let deleteMBraceDeployment (logger : ISystemLogger) (serviceName:string) (client:SubscriptionClient) = async {
        let! service = client.Compute.HostedServices.GetDetailedAsync serviceName |> Async.AwaitTaskCorrect
        if service.Properties.ExtendedProperties |> Common.isMBraceAsset then
            logger.Logf LogLevel.Info "deleting cluster %s" serviceName
            let! result = client.Compute.Deployments.DeleteByNameAsync(serviceName, serviceName, true) |> Async.AwaitTaskCorrect |> Async.Catch
            match result with
            | Choice1Of2 deleteOp when deleteOp.Status = OperationStatus.Succeeded -> ()
            | Choice1Of2 deleteOp -> return invalidOp <| sprintf "Failed to delete deployment %A: %s" serviceName deleteOp.Error.Message
            | Choice2Of2 _ -> logger.Logf LogLevel.Warning "No deployment for cloud service %A could be found." serviceName

            let! deleteOp = client.Compute.HostedServices.DeleteAsync serviceName |> Async.AwaitTaskCorrect
            if deleteOp.StatusCode <> Net.HttpStatusCode.OK then return failwith (string deleteOp.StatusCode)
        else
            logger.Logf LogLevel.Info "No MBrace cluster called %A found" serviceName
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