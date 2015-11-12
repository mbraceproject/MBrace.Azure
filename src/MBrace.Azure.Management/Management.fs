namespace MBrace.Azure.Management

open System
open System.Collections.Generic
open System.IO
open System.Security.Cryptography.X509Certificates
open System.Text.RegularExpressions
open System.Xml.Linq

open Microsoft.Azure

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils.PrettyPrinters
open MBrace.Azure
open MBrace.Azure.Runtime

module internal Common =

    let defaultMBraceVersion = System.AssemblyVersionInformation.ReleaseTag

    let resourcePrefix = "mbrace"
    let generateResourceName() = resourcePrefix + Guid.NewGuid().ToString("N").[..7]

    let getPackageUrl mbraceNugetVersionTag (vmSize : VMSize) = 
        sprintf "https://github.com/mbraceproject/MBrace.Azure/releases/download/%s/MBrace.Azure.CloudService-%s.cspkg" 
                mbraceNugetVersionTag vmSize.Id

    let defaultExtendedProperties = dict [ "IsMBraceAsset", "true"]
    let isMBraceAsset (extendedProperties:IDictionary<string, string>) = extendedProperties.ContainsKey "IsMBraceAsset"

module internal Infrastructure =
    open Microsoft.WindowsAzure.Management
    open Microsoft.WindowsAzure.Management.Models

    /// fetches a list of all regions together with supported vm sizes
    let listRegions (client:SubscriptionClient) = async {
        let requiredServices = [ "Compute" ; "Storage" ; "ServiceBus" ]
        let! (listedRoleSizes : RoleSizeListResponse) = client.Management.RoleSizes.ListAsync()
        let! (locations : LocationsListResponse) = client.Management.Locations.ListAsync()
        let rolesForClient = listedRoleSizes |> Seq.map (fun r -> r.Name) |> set
        return 
            locations
            |> Seq.filter(fun l -> requiredServices |> List.forall l.AvailableServices.Contains)
            |> Seq.map(fun l -> l.Name, l.ComputeCapabilities.WebWorkerRoleSizes |> Seq.filter rolesForClient.Contains |> Seq.toList)
            |> Seq.toList
    }

module internal Storage =

    open Microsoft.WindowsAzure.Management.Storage
    open Microsoft.WindowsAzure.Management.Storage.Models

    let private connectionStringRegex = new Regex("DefaultEndpointsProtocol=https;AccountName=(.+);AccountKey=(.+)", RegexOptions.Compiled)
    let tryParseConnectionString (conn : string) =
        let m = connectionStringRegex.Match(conn)
        if m.Success then
            let accountName = m.Groups.[1].Value
            let accountKey = m.Groups.[2].Value
            Some(accountName, accountKey)
        else
            None

    let mkConnectionString accountName (key : string) =
        sprintf "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s" accountName key

    let getStorageAccounts (client:SubscriptionClient) = async {
        let! (listed : StorageAccountListResponse) = client.Storage.StorageAccounts.ListAsync()
        return listed |> Seq.toArray
    }

    let tryFindMBraceStorage (region : Region) (client:SubscriptionClient) = async {
        let! accounts = getStorageAccounts client
        return
            accounts
            |> Seq.filter (fun account -> 
                let hasLocationData, storageAccountLocation = account.ExtendedProperties.TryGetValue "ResourceLocation"
                hasLocationData && storageAccountLocation = region.Id)
            |> Seq.filter(fun account -> account.ExtendedProperties |> Common.isMBraceAsset)
            |> Seq.map(fun account -> account.Name)
            |> Seq.tryPick Some
    }

    let rec createMBraceStorageAccount (logger : ISystemLogger) (region : Region) (accountName : string) (client:SubscriptionClient) = async {
        let! (availability : CheckNameAvailabilityResponse) = client.Storage.StorageAccounts.CheckNameAvailabilityAsync accountName 
        if not availability.IsAvailable then return! createMBraceStorageAccount logger region accountName client
        else
            let storageParams = new StorageAccountCreateParameters(Name = accountName, AccountType = "Standard_LRS", Location = region.Id, ExtendedProperties = Common.defaultExtendedProperties)
            let! (result : OperationStatusResponse) = client.Storage.StorageAccounts.CreateAsync(storageParams)
            if result.Status = OperationStatus.Failed then 
                return invalidOp result.Error.Message
            else 
                logger.Logf LogLevel.Info "Created new storage account %A" accountName
                return accountName 
    }

    let getAccountInfo (id : string) (client:SubscriptionClient) = async {
        match tryParseConnectionString id with
        | Some(accountName, _) -> 
            // input identified as connection string, parse and return account name
            return accountName, id
        | None ->
            // input identifier as account name, recover connection string from Storage Account client
            let! (keys : StorageAccountGetKeysResponse) = client.Storage.StorageAccounts.GetKeysAsync id
            let conn = mkConnectionString id keys.PrimaryKey
            return id, conn
    }

    let getDeploymentContainer connectionString = async {
        let account = AzureStorageAccount.Parse connectionString
        let container = account.BlobClient.GetContainerReference "deployments"
        do! container.CreateIfNotExistsAsync()
        return container
    }

    let getDefaultMBraceStorageAccountName (logger : ISystemLogger) (region : Region) client = async {
        let! result = tryFindMBraceStorage region client
        match result with
        | Some storage -> 
            logger.Logf LogLevel.Info "Reusing existing storage account %s" storage
            return storage
        | None -> 
            let accountName = Common.generateResourceName()
            return! createMBraceStorageAccount logger region accountName client
    }

    let resolveStorageAccount (logger : ISystemLogger) (region : Region) (storageAccount : string option) (client : SubscriptionClient) = async {
        match storageAccount with
        | Some account -> 
            // parse and validate storage account info
            return! getAccountInfo account client
        | None ->
            let! accountName = getDefaultMBraceStorageAccountName logger region client
            return! getAccountInfo accountName client
    }

module internal ServiceBus =

    open Microsoft.WindowsAzure.Management.ServiceBus
    open Microsoft.WindowsAzure.Management.ServiceBus.Models

    let mkEndpoint (namespaceName : string) =
        sprintf "sb://%s.servicebus.windows.net/" namespaceName

    let mkConnectionString endpoint (key : string) =
        sprintf "EndPoint=%s;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=%s" endpoint key

    let private connectionStringRegex = new Regex("Endpoint=(.+);SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=(.+)", RegexOptions.Compiled)
    let tryParseConnectionString (conn : string) =
        let m = connectionStringRegex.Match(conn)
        if m.Success then
            let endPoint = m.Groups.[1].Value |> Uri
            let namespaceName = endPoint.Host.Split('.').[0]
            let accountKey = m.Groups.[2].Value
            Some(endPoint.ToString(), namespaceName, accountKey)
        else
            None

    let tryParseEndpoint (endpoint : string) =
        let ok, uri = Uri.TryCreate(endpoint, UriKind.Absolute)
        if ok && uri.Scheme = "sb" then Some (uri.Host.Split('.').[0])
        else None

    let rec waitUntilState checkState ns (client:SubscriptionClient) = async {
        let! result = client.ServiceBus.Namespaces.GetAsync ns |> Async.AwaitTaskCorrect |> Async.Catch
        let status = match result with Choice1Of2 ns -> ns.Namespace.Status | Choice2Of2 _ -> ""
        if not (checkState status) then
            do! Async.Sleep 2000
            return! waitUntilState checkState ns client
    }

    let rec createNamespace (logger : ISystemLogger) (region : Region) (namespaceName : string) (client:SubscriptionClient) = async {
        logger.Logf LogLevel.Info "checking availability of service bus namespace %s" namespaceName
        let! (availability : CheckNamespaceAvailabilityResponse) = client.ServiceBus.Namespaces.CheckAvailabilityAsync namespaceName
        if not availability.IsAvailable then 
            return! createNamespace logger region namespaceName client
        else
            logger.Logf LogLevel.Info "creating service bus namespace %s" namespaceName
            let result = client.ServiceBus.Namespaces.Create(namespaceName, region.Id) 

            do! client |> waitUntilState ((=) "Active") namespaceName

            if result.StatusCode <> Net.HttpStatusCode.OK then 
                return failwithf "Failed to create service bus: %O" result.StatusCode
            else 
                logger.Logf LogLevel.Info "Created new default MBrace Service Bus namespace %s" namespaceName
                return namespaceName 
    }

    let getNamespaces (client:SubscriptionClient) = async {
        let! (nss : ServiceBusNamespacesResponse) = client.ServiceBus.Namespaces.ListAsync()
        return nss |> Seq.toArray
    }

    let tryFindMBraceNamespace (region : Region) (client:SubscriptionClient) = async {
        let! accounts = getNamespaces client
        return
            accounts
            |> Seq.filter (fun ns -> ns.Region = region.Id)
            |> Seq.filter(fun ns -> ns.Name.StartsWith Common.resourcePrefix)
            |> Seq.map(fun ns -> ns.Name)
            |> Seq.tryPick Some
    }

    let getAccountInfo (id : string) (client:SubscriptionClient) = async {
        match tryParseConnectionString id with
        | Some(endpoint, namespaceName, _) -> 
            // input identified as connection string, parse and return account name
            return namespaceName, endpoint, id
        | None ->
            let namespaceName = defaultArg (tryParseEndpoint id) id
            // input identifier as account name, recover connection string from Storage Account client
            let! (authRules : ServiceBusAuthorizationRulesResponse) = client.ServiceBus.Namespaces.ListAuthorizationRulesAsync(namespaceName)
            let rootSharedAccessKey = authRules |> Seq.find (fun rule -> rule.KeyName = "RootManageSharedAccessKey")
            let endpoint = mkEndpoint namespaceName
            let connectionString = mkConnectionString endpoint rootSharedAccessKey.PrimaryKey
            return namespaceName, endpoint, connectionString
    }

    let findOrCreateMBraceNamespace (logger : ISystemLogger) region client = async {
        let! nsOpt = tryFindMBraceNamespace region client
        match nsOpt with
        | Some ns -> 
            logger.Logf LogLevel.Info "Reusing existing Service Bus namespace %A" ns
            return ns
        | None -> 
            let namespaceName = Common.generateResourceName()
            return! createNamespace logger region namespaceName client
    }

    let resolveNamespaceInfo (logger : ISystemLogger) (region : Region) (serviceBusId : string option) (client : SubscriptionClient) = async {
        match serviceBusId with
        | Some id -> return! getAccountInfo id client
        | None ->
            let! ns = findOrCreateMBraceNamespace logger region client
            return! getAccountInfo ns client    
    }

    let deleteNamespace namespaceName (client:SubscriptionClient) = async {
        let! _response = client.ServiceBus.Namespaces.DeleteAsync namespaceName
        do! client |> waitUntilState ((<>) "Removing") namespaceName
    }

module internal Deployment =

    open Microsoft.WindowsAzure.Management.Compute
    open Microsoft.WindowsAzure.Management.Compute.Models

    type Node = { 
        Id : string
        Hostname : string
        VMSize : VMSize 
        Status : string }

    type DeploymentDetails = {
        Name : string
        CreatedTime : DateTime
        ServiceStatus : string
        DeploymentStatus : string option
        Nodes : Node list }

    let validateServiceName (client:SubscriptionClient) serviceName = async { 
        let! (result : HostedServiceCheckNameAvailabilityResponse) = client.Compute.HostedServices.CheckNameAvailabilityAsync serviceName
        if not result.IsAvailable then return invalidOp result.Reason
    }

    type DeploymentReporter private () =
        static let template : Field<DeploymentDetails> list =
            [ 
                Field.create "Name" Left (fun d -> d.Name)
                Field.create "VM size" Left (fun d -> match d.Nodes with [] -> "N/A" | h :: _ -> h.VMSize.Id)
                Field.create "Instance count" Right (fun d -> if List.isEmpty d.Nodes then "N/A" else string d.Nodes.Length)
                Field.create "Created Time" Left (fun d -> d.CreatedTime) 
                Field.create "Service Status" Left (fun d -> d.ServiceStatus) 
                Field.create "Deployment State" Left (fun d -> defaultArg d.DeploymentStatus null)
            ]

        static member Report(deployments : DeploymentDetails list, ?title : string) =
            Record.PrettyPrint(template, deployments, ?title = title, useBorders = false)

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

    let getRunningDeployments (client:SubscriptionClient) = async {
        let! (services : HostedServiceListResponse) = client.Compute.HostedServices.ListAsync()
        let getHostedServiceInfo (service : HostedServiceListResponse.HostedService) = async {
            if service.Properties.ExtendedProperties |> Common.isMBraceAsset then 
                let! deployment = async {
                    try
                        let ds = client.Compute.Deployments
                        let! (d : DeploymentGetResponse) = ds.GetBySlotAsync(service.ServiceName, DeploymentSlot.Production)
                        return Some d
                    with _ -> return None
                }

                let nodes =
                    match deployment with
                    | None -> []
                    | Some d -> 
                        d.RoleInstances 
                        |> Seq.map (fun i -> { Id = i.InstanceName ; Hostname = i.HostName ; VMSize = VMSize.Define i.InstanceSize ; Status = i.InstanceStatus }) 
                        |> Seq.toList

                let info = 
                    {   Name = service.ServiceName
                        CreatedTime = service.Properties.DateCreated
                        ServiceStatus = service.Properties.Status.ToString()
                        DeploymentStatus = deployment |> Option.map(fun deployment -> deployment.Status.ToString())
                        Nodes = nodes } 

                return Some info
            else
                return None
        }

        let! info = services |> Seq.map getHostedServiceInfo |> Async.Parallel 
        return info |> Seq.choose id |> Seq.toList
    }

    let buildMBraceConfig serviceName instances storageConnection serviceBusConnection =
        sprintf """<?xml version="1.0" encoding="utf-8"?>
<ServiceConfiguration serviceName="%s" xmlns="http://schemas.microsoft.com/ServiceHosting/2008/10/ServiceConfiguration" osFamily="4" osVersion="*" schemaVersion="2015-04.2.6">
    <Role name="MBrace.Azure.WorkerRole">
    <Instances count="%d" />
    <ConfigurationSettings>
        <Setting name="MBrace.StorageConnectionString" value="%s" />
        <Setting name="MBrace.ServiceBusConnectionString" value="%s" />
        <Setting name="Microsoft.WindowsAzure.Plugins.Diagnostics.ConnectionString" value="" />
    </ConfigurationSettings>
    </Role>
</ServiceConfiguration>""" serviceName instances storageConnection serviceBusConnection

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

    let deploy (slot : DeploymentSlot) (deployParams : DeploymentCreateParameters) (client : SubscriptionClient) = async {
        let! (createOp : OperationStatusResponse) = client.Compute.Deployments.CreateAsync(deployParams.Name, slot, deployParams)
        if createOp.StatusCode <> Net.HttpStatusCode.Accepted then 
            return failwithf "error: HTTP request for creation operation %A was not accepted (status code: %O)" deployParams.Name createOp.StatusCode
    }            

    let deleteMBraceDeployment (logger : ISystemLogger) (serviceName:string) (client:SubscriptionClient) = async {
        let! (service : HostedServiceGetDetailedResponse) = client.Compute.HostedServices.GetDetailedAsync serviceName
        if service.Properties.ExtendedProperties |> Common.isMBraceAsset then
            logger.Logf LogLevel.Info "deleting cluster %s" serviceName
            let! (deleteOp : OperationStatusResponse) = client.Compute.Deployments.DeleteByNameAsync(serviceName, serviceName, true)
            if deleteOp.Status <> OperationStatus.Succeeded then return failwith deleteOp.Error.Message
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