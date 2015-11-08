namespace MBrace.Azure

open System
open System.Collections.Generic
open System.IO
open System.Security.Cryptography.X509Certificates
open System.Text.RegularExpressions
open System.Xml.Linq

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Azure.Runtime

open Microsoft.Azure
open Microsoft.WindowsAzure.Management
open Microsoft.WindowsAzure.Management.Compute
open Microsoft.WindowsAzure.Management.Compute.Models
open Microsoft.WindowsAzure.Management.Storage
open Microsoft.WindowsAzure.Management.Storage.Models
open Microsoft.WindowsAzure.Management.ServiceBus
open Microsoft.WindowsAzure.Management.ServiceBus.Models

/// Azure Region string identifier
type Region = string
/// Azure VM type string identifier
type VMSize = string

/// Collection of preset Azure Region identifiers
type Regions = 
    static member South_Central_US  : Region = "South Central US"
    static member West_US           : Region = "West US"
    static member Central_US        : Region = "Central US"
    static member East_US           : Region = "East US"
    static member East_US_2         : Region = "East US 2"
    static member North_Europe      : Region = "North Europe"
    static member West_Europe       : Region = "West Europe"
    static member Southeast_Asia    : Region = "Southeast Asia"
    static member East_Asia         : Region = "East Asia"

/// Collection of preset Azure VM type identifiers
type VMSizes = 
    static member A10               : VMSize = "A10"
    static member A11               : VMSize = "A11"
    static member A5                : VMSize = "A5"
    static member A6                : VMSize = "A6"
    static member A7                : VMSize = "A7"
    static member A8                : VMSize = "A8"
    static member A9                : VMSize = "A9"
    static member A4                : VMSize = "ExtraLarge"
    static member A0                : VMSize = "ExtraSmall"
    static member A3                : VMSize = "Large"
    static member A2                : VMSize = "Medium"
    static member A1                : VMSize = "Small"
    static member Extra_Large       : VMSize = "ExtraLarge"
    static member Large             : VMSize = "Large"
    static member Medium            : VMSize = "Medium"
    static member Small             : VMSize = "Small"
    static member Extra_Small       : VMSize = "ExtraSmall"
    static member Standard_D1       : VMSize = "Standard_D1"
    static member Standard_D11      : VMSize = "Standard_D11"
    static member Standard_D11_v2   : VMSize = "Standard_D11_v2"
    static member Standard_D12      : VMSize = "Standard_D12"
    static member Standard_D12_v2   : VMSize = "Standard_D12_v2"
    static member Standard_D13      : VMSize = "Standard_D13"
    static member Standard_D13_v2   : VMSize = "Standard_D13_v2"
    static member Standard_D14      : VMSize = "Standard_D14"
    static member Standard_D14_v2   : VMSize = "Standard_D14_v2"
    static member Standard_D1_v2    : VMSize = "Standard_D1_v2"
    static member Standard_D2       : VMSize = "Standard_D2"
    static member Standard_D2_v2    : VMSize = "Standard_D2_v2"
    static member Standard_D3       : VMSize = "Standard_D3"
    static member Standard_D3_v2    : VMSize = "Standard_D3_v2"
    static member Standard_D4       : VMSize = "Standard_D4"
    static member Standard_D4_v2    : VMSize = "Standard_D4_v2"
    static member Standard_D5_v2    : VMSize = "Standard_D5_v2"

/// Azure subscription record
[<NoEquality; NoComparison; AutoSerializable(false)>]
type Subscription = 
    { 
        /// Human-readable subscription name
        Name : string
        /// Subscription identifier
        Id : string  
        /// X509 management certificate
        ManagementCertificate : string
        /// Azure service management url
        ServiceManagementUrl : string
    }

type PublishSettings =
    {
        Subscriptions : Subscription []
    }

    member ps.Item (subscriptionId : string) =
        ps.Subscriptions |> Array.find (fun s -> s.Id = subscriptionId || s.Name.Contains subscriptionId)

    /// Parse publish settings found in given xml string
    static member Parse(xml : string) : PublishSettings = 
        let parseSubscription (elem : XElement) = 
            let name = elem.Attribute(XName.op_Implicit "Name").Value
            let id = elem.Attribute(XName.op_Implicit "Id").Value
            let mc = elem.Attribute(XName.op_Implicit "ManagementCertificate").Value
            let smu = elem.Attribute(XName.op_Implicit "ServiceManagementUrl").Value
            {   Name = name;
                Id = id;
                ManagementCertificate = mc
                ServiceManagementUrl = smu }

        let doc = XDocument.Parse xml
        let pubData = doc.Element(XName.op_Implicit "PublishData")
        let pubProfile = pubData.Element(XName.op_Implicit "PublishProfile")
        let subscriptions = [| for s in pubProfile.Elements(XName.op_Implicit "Subscription") -> parseSubscription s |]
        { Subscriptions = subscriptions }

    /// Parse publish settings from given local file path
    static member ParseFile(publishSettingsFile : string) : PublishSettings =
        PublishSettings.Parse(File.ReadAllText publishSettingsFile)

[<AutoOpen>]
module private ManagementImpl = 

    let urlForPackage mbraceNugetVersionTag vmSize = 
        sprintf "https://github.com/mbraceproject/MBrace.Azure/releases/download/%s/MBrace.Azure.CloudService-%s.cspkg" mbraceNugetVersionTag vmSize

    let resourcePrefix = "mbrace"
    let generateResourceName() = resourcePrefix + Guid.NewGuid().ToString("N").[..7]
    let defaultMBraceVersion = System.AssemblyVersionInformation.ReleaseTag
    let defaultExtendedProperties = dict [ "IsMBraceAsset", "true"]
    let isMBraceAsset (extendedProperties:IDictionary<string, string>) = extendedProperties.ContainsKey "IsMBraceAsset"

    [<NoEquality; NoComparison; AutoSerializable(false)>]
    type SubscriptionClient =
        {
            Subscription : Subscription
            Credentials : CertificateCloudCredentials
            Storage : StorageManagementClient
            ServiceBus : ServiceBusManagementClient
            Compute : ComputeManagementClient
            Management : ManagementClient 
        }

        static member Activate(subscription : Subscription) =
            let cert = new X509Certificate2(Convert.FromBase64String subscription.ManagementCertificate)
            let cred = new CertificateCloudCredentials(subscription.Id, cert)
            { Subscription = subscription
              Credentials = cred
              Storage = new StorageManagementClient(cred)
              ServiceBus = new ServiceBusManagementClient(cred)
              Compute = new ComputeManagementClient(cred)
              Management = new ManagementClient(cred) }

    type PubSettingsClient =
        {
            Default : SubscriptionClient
            Subscriptions : SubscriptionClient []
        }

        member c.GetClientByIdOrDefault(?id : string) =
            match id with
            | None -> c.Default
            | Some id -> c.Subscriptions |> Array.find (fun s -> s.Subscription.Id = id || s.Subscription.Name.Contains id)

        member c.Item with get (id : string) = c.GetClientByIdOrDefault(id = id)

        static member Activate(subscriptions : seq<Subscription>, ?defaultSubscriptionId : string) =
            match subscriptions |> Seq.distinctBy (fun s -> s.Id) |> Seq.toArray with
            | [||] -> invalidArg "subscriptions" "supplied an empty set of Azure subscriptions."
            | subscriptions ->
                let clients = subscriptions |> Array.map SubscriptionClient.Activate
                let defaultSubscriptionId = defaultArg defaultSubscriptionId subscriptions.[0].Id
                let defaultSubscription = clients |> Array.find (fun c -> c.Subscription.Id = defaultSubscriptionId || c.Subscription.Name.Contains defaultSubscriptionId)
                { Default = defaultSubscription
                  Subscriptions = clients }

    module Infrastructure =
        let listRegions (client:SubscriptionClient) =
            let requiredServices = [ "Compute"; "Storage" ]
            let rolesForClient = set(client.Management.RoleSizes.List() |> Seq.map(fun r -> r.Name))
            client.Management.Locations.List()
            |> Seq.filter(fun location -> requiredServices |> List.forall(location.AvailableServices.Contains))
            |> Seq.map(fun location ->
                let rolesInLocation = set location.ComputeCapabilities.WebWorkerRoleSizes
                location.Name, rolesForClient |> Set.intersect rolesInLocation |> Seq.toList)
            |> Seq.toList


    module Storage =

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

        let tryFindMBraceStorage region (client:SubscriptionClient) = async {
            let! accounts = getStorageAccounts client
            return
                accounts
                |> Seq.filter (fun account -> 
                    let hasLocationData, storageAccountLocation = account.ExtendedProperties.TryGetValue "ResourceLocation"
                    hasLocationData && storageAccountLocation = region)
                |> Seq.filter(fun account -> account.ExtendedProperties |> isMBraceAsset)
                |> Seq.map(fun account -> account.Name)
                |> Seq.tryPick Some
        }

        let rec createMBraceStorageAccount (logger : ISystemLogger) (region : string) (accountName : string) (client:SubscriptionClient) = async {
            let! (availability : CheckNameAvailabilityResponse) = client.Storage.StorageAccounts.CheckNameAvailabilityAsync accountName 
            if not availability.IsAvailable then return! createMBraceStorageAccount logger region accountName client
            else
                let storageParams = new StorageAccountCreateParameters(Name = accountName, AccountType = "Standard_LRS", Location = region, ExtendedProperties = defaultExtendedProperties)
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

        let getDefaultMBraceStorageAccountName (logger : ISystemLogger) region client = async {
            let! result = tryFindMBraceStorage region client
            match result with
            | Some storage -> 
                logger.Logf LogLevel.Info "Reusing existing storage account %s" storage
                return storage
            | None -> 
                let accountName = generateResourceName()
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

    module ServiceBus =

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
                let result = client.ServiceBus.Namespaces.Create(namespaceName, region) 

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
                |> Seq.filter (fun ns -> ns.Region = region)
                |> Seq.filter(fun ns -> ns.Name.StartsWith resourcePrefix)
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
                let namespaceName = generateResourceName()
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

    module Clusters =
        open System.IO

        type Node = { Size : string; Status : string }
        type ClusterDetails = {
            Name : string
            CreatedTime : DateTime
            ServiceStatus : string
            DeploymentStatus : string option
            Nodes : Node list }

        let validateClusterName (client:SubscriptionClient) clusterName = async { 
            let! (result : HostedServiceCheckNameAvailabilityResponse) = client.Compute.HostedServices.CheckNameAvailabilityAsync clusterName
            if not result.IsAvailable then return invalidOp result.Reason
        }

        let getConnectionStrings clusterName (client:SubscriptionClient) = async {
            let! (service : HostedServiceGetDetailedResponse) = client.Compute.HostedServices.GetDetailedAsync clusterName
            let storageConnectionString = service.Properties.ExtendedProperties.["StorageAccountConnectionString"]
            let serviceBusConnectionString = service.Properties.ExtendedProperties.["ServiceBusConnectionString"]
            let serviceBusNamespace = service.Properties.ExtendedProperties.["ServiceBusName"]
            return storageConnectionString, serviceBusConnectionString, serviceBusNamespace
        }

        let getRunningMBraceClusters (client:SubscriptionClient) = async {
            let! (services : HostedServiceListResponse) = client.Compute.HostedServices.ListAsync()
            let getHostedServiceInfo (service : HostedServiceListResponse.HostedService) =
                if service.Properties.ExtendedProperties |> isMBraceAsset then 
                    let deployment =
                        try client.Compute.Deployments.GetBySlot(service.ServiceName, DeploymentSlot.Production) |> Some
                        with _ -> None

                    let info = 
                        {   Name = service.ServiceName
                            CreatedTime = service.Properties.DateCreated
                            ServiceStatus = service.Properties.Status.ToString()
                            DeploymentStatus = deployment |> Option.map(fun deployment -> deployment.Status.ToString())
                            Nodes =
                                deployment
                                |> Option.map(fun deployment ->
                                    deployment.RoleInstances
                                    |> Seq.map(fun instance -> { Size = instance.InstanceSize; Status = instance.InstanceStatus })
                                    |> Seq.toList)
                                |> defaultArg <| [] 
                        } 

                    Some info
                else
                    None

            return services |> Seq.choose getHostedServiceInfo |> Seq.toList
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

        let prepareMBraceServiceDeployment (logger : ISystemLogger) (clusterName : string) (clusterLabel : string) 
                                            (region : Region) (packagePath : string) (config : string) 
                                            (storageAccountName : string) (storageConnectionString : string) 
                                            (serviceBusNamespace : string) (serviceBusConnectionString : string) (client:SubscriptionClient) = async {

            let extendedProperties =
                [ "StorageAccountName", storageAccountName
                  "StorageAccountConnectionString", storageConnectionString
                  "ServiceBusName", serviceBusNamespace
                  "ServiceBusConnectionString", serviceBusConnectionString ]
                @ (defaultExtendedProperties.Keys |> Seq.map(fun key -> key, defaultExtendedProperties.[key]) |> Seq.toList)
                |> dict

            logger.Logf LogLevel.Info "creating cloud service %s" clusterName
            let! _ = client.Compute.HostedServices.CreateAsync(HostedServiceCreateParameters(Location = region, ServiceName = clusterName, ExtendedProperties = extendedProperties))

            let! container = Storage.getDeploymentContainer storageConnectionString
            let packageBlob = packagePath |> Path.GetFileName |> container.GetBlockBlobReference
            let blobSizesDoNotMatch() =
                packageBlob.FetchAttributes()
                packageBlob.Properties.Length <> FileInfo(packagePath).Length

            if (not (packageBlob.Exists()) || blobSizesDoNotMatch()) then
                logger.Logf LogLevel.Info "uploading package %A" packagePath
                do! packageBlob.UploadFromFileAsync(packagePath, FileMode.Open)
        
            logger.Logf LogLevel.Info "scheduling cluster creation:\n  cluster %s\n  package uri %s\n  config %s" clusterName (packageBlob.Uri.ToString()) config
            return DeploymentCreateParameters(
                Label = clusterLabel,
                Name = clusterName,
                PackageUri = packageBlob.Uri,
                Configuration = config,
                StartDeployment = Nullable true,
                TreatWarningsAsError = Nullable true)
          }

        let beginDeploy (slot : DeploymentSlot) (deployParams : DeploymentCreateParameters) (client : SubscriptionClient) = async {
            let! (createOp : AzureOperationResponse) = client.Compute.Deployments.BeginCreatingAsync(deployParams.Name, slot, deployParams)
            if createOp.StatusCode <> Net.HttpStatusCode.Accepted then 
                return failwith "error: HTTP request for creation operation was not accepted"
        }

        let deploy (slot : DeploymentSlot) (deployParams : DeploymentCreateParameters) (client : SubscriptionClient) = async {
            let! (createOp : OperationStatusResponse) = client.Compute.Deployments.CreateAsync(deployParams.Name, slot, deployParams)
            if createOp.StatusCode <> Net.HttpStatusCode.Accepted then 
                return failwith "error: HTTP request for creation operation was not accepted"
        }            

        let deleteMBraceCluster (logger : ISystemLogger) (clusterName:string) (client:SubscriptionClient) = async {
            let service = client.Compute.HostedServices.GetDetailed clusterName
            if service.Properties.ExtendedProperties |> isMBraceAsset then
                logger.Logf LogLevel.Info "deleting cluster %s" clusterName
                let deleteOp = client.Compute.Deployments.DeleteByName(clusterName, clusterName, true)
                if deleteOp.Status <> OperationStatus.Succeeded then return failwith deleteOp.Error.Message
                let deleteOp = client.Compute.HostedServices.Delete clusterName
                if deleteOp.StatusCode <> Net.HttpStatusCode.OK then return failwith (deleteOp.StatusCode.ToString()) 
            else
                logger.Logf LogLevel.Info "No MBrace cluster called %A found" clusterName
        }

        let downloadServicePackage (logger : ISystemLogger) (vmSize : VMSize) (mbraceVersion : string option) (uri : string option) = async {
            let uri, version =
                match uri with
                | Some u -> Uri u, None
                | None ->
                    let mbraceVersion = defaultArg mbraceVersion defaultMBraceVersion
                    urlForPackage mbraceVersion vmSize |> Uri, Some mbraceVersion

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

[<Sealed; AutoSerializable(false)>]
type ClusterManager private (pubSettings : PubSettingsClient, defaultRegion : Region, _logger : ISystemLogger option, ?logLevel : LogLevel) =

    let logger = AttacheableLogger.Create(?logLevel = logLevel, makeAsynchronous = false)
    do _logger |> Option.iter(fun l -> ignore <| logger.AttachLogger l)

    let mutable defaultRegion = defaultRegion

    member __.AttachLogger(l : ISystemLogger) = logger.AttachLogger l
    member __.DefaultRegion
        with get () = defaultRegion
        and set reg = defaultRegion <- reg

    member __.Subscriptions = pubSettings.Subscriptions |> Array.map (fun s -> s.Subscription.Name)
    member __.DefaultSubscription = pubSettings.Default.Subscription.Name

    static member Create(subscriptions : seq<Subscription>, defaultRegion : Region, ?defaultSubscriptionId : string, ?logger : ISystemLogger, ?logLevel : LogLevel) =
        let client = PubSettingsClient.Activate(subscriptions, ?defaultSubscriptionId = defaultSubscriptionId)
        new ClusterManager(client, defaultRegion, logger, ?logLevel = logLevel)

    static member Create(subscription : Subscription, defaultRegion : Region, ?logger : ISystemLogger, ?logLevel : LogLevel) =
        ClusterManager.Create([subscription], defaultRegion, ?logger = logger, ?logLevel = logLevel)

    static member FromPublishSettingsFile(publishSettingsFile : string, defaultRegion : Region, ?defaultSubscriptionId : string, ?logger : ISystemLogger, ?logLevel : LogLevel) =
        let pubSettings = PublishSettings.ParseFile publishSettingsFile
        ClusterManager.Create(pubSettings.Subscriptions, defaultRegion, ?defaultSubscriptionId = defaultSubscriptionId, ?logger = logger, ?logLevel = logLevel)

    member __.CreateClusterAsync(clusterName : string, vmSize : VMSize, vmCount : int, ?subscriptionId : string, ?region : Region, 
                                        ?mbraceVersion : string, ?storageAccount : string, ?serviceBusAccount : string, ?cloudServicePackage : string, ?clusterLabel : string) = async {

        if vmCount < 1 then invalidOp "vmCount" "Must be positive value."
        let region = defaultArg region defaultRegion
        let client = pubSettings.GetClientByIdOrDefault(?id = subscriptionId)
        do! Clusters.validateClusterName client clusterName

        logger.Logf LogLevel.Info "using vm size %s" vmSize
        let! packagePath, versionInfo = Clusters.downloadServicePackage logger vmSize mbraceVersion cloudServicePackage
        logger.Logf LogLevel.Info "using cluster name %s" clusterName

        let! storageAccountName, storageConnectionString = Storage.resolveStorageAccount logger region storageAccount client
        logger.Logf LogLevel.Info "using storage account name %A" storageAccountName
        let! _, serviceBusNamespace, serviceBusConnectionString = ServiceBus.resolveNamespaceInfo logger region serviceBusAccount client
        logger.Logf LogLevel.Info "using service bus account %A" serviceBusNamespace

        let config = Clusters.buildMBraceConfig clusterName vmCount storageConnectionString serviceBusConnectionString

        let clusterLabel = defaultArg clusterLabel (sprintf "MBrace cluster %A, package %s"  clusterName (defaultArg versionInfo "custom"))
        let! deployInfo = Clusters.prepareMBraceServiceDeployment logger clusterName clusterLabel region packagePath config storageAccountName storageConnectionString serviceBusNamespace serviceBusConnectionString client
        do! Clusters.beginDeploy DeploymentSlot.Production deployInfo client
        return new Configuration(storageConnectionString, serviceBusConnectionString)
    }

    member __.CreateCluster(clusterName : string, vmSize : VMSize, vmCount : int, ?subscriptionId : string, ?region : Region, 
                                        ?mbraceVersion : string, ?storageAccount : string, ?serviceBusAccount : string, ?cloudServicePackage : string, ?clusterLabel : string) =
        __.CreateClusterAsync(clusterName, vmSize, vmCount, ?subscriptionId = subscriptionId, ?region = region, ?mbraceVersion = mbraceVersion,
                                ?storageAccount = storageAccount, ?serviceBusAccount = serviceBusAccount, ?cloudServicePackage = cloudServicePackage, ?clusterLabel = clusterLabel)
        |> Async.RunSync

    member __.DeleteClusterAsync(clusterName : string, ?subscriptionId : string) = async { 
        let client = pubSettings.GetClientByIdOrDefault(?id = subscriptionId) 
        return! Clusters.deleteMBraceCluster logger clusterName client
    }

    member __.DeleteCluster(clusterName : string, ?subscriptionId : string) =
        __.DeleteClusterAsync(clusterName, ?subscriptionId = subscriptionId)
        |> Async.RunSync

    member __.GetClustersAsync(?subscriptionId : string) = async { 
        let client = pubSettings.GetClientByIdOrDefault(?id = subscriptionId)
        let! clusters = Clusters.getRunningMBraceClusters client
        return clusters |> List.map (sprintf "%+A")
    }

    member __.GetClusters(?subscriptionId : string) =
        __.GetClustersAsync(?subscriptionId = subscriptionId)
        |> Async.RunSync