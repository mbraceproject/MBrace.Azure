namespace MBrace.Azure

open System
open System.Collections.Generic
open System.IO
open System.Security.Cryptography.X509Certificates
open System.Text.RegularExpressions
open System.Xml.Linq

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils.PrettyPrinters
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

/// Parsed PublishSettings record
[<NoEquality; NoComparison; AutoSerializable(false)>]
type PublishSettings =
    {
        /// Set of Azure subscriptions defined in PubSettings
        Subscriptions : Subscription []
    }

    /// Look up subscription by id or name
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
        let listRegions (client:SubscriptionClient) = async {
            let requiredServices = [ "Compute"; "Storage" ]
            let! (listedRoleSizes : Models.RoleSizeListResponse) = client.Management.RoleSizes.ListAsync()
            let! (locations : Models.LocationsListResponse) = client.Management.Locations.ListAsync()
            let rolesForClient = listedRoleSizes |> Seq.map (fun r -> r.Name) |> set
            return 
                locations
                |> Seq.filter(fun location -> requiredServices |> List.forall(location.AvailableServices.Contains))
                |> Seq.map(fun location ->
                    let rolesInLocation = set location.ComputeCapabilities.WebWorkerRoleSizes
                    location.Name, rolesForClient |> Set.intersect rolesInLocation |> Seq.toList)
                |> Seq.toList
        }

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

    module Deployment =
        open System.IO

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
                [ Field.create "Name" Left (fun d -> d.Name)
                  Field.create "Created Time" Left (fun d -> d.CreatedTime) 
                  Field.create "Service Status" Left (fun d -> d.ServiceStatus) 
                  Field.create "Deployment State" Left (fun d -> defaultArg d.DeploymentStatus null)
                  Field.create "VM size" Left (fun d -> match d.Nodes with [] -> "N/A" | h :: _ -> h.VMSize)
                  Field.create "Instance count" Left (fun d -> if List.isEmpty d.Nodes then "N/A" else string d.Nodes.Length) ]

            static member Report(deployments : DeploymentDetails list, ?title : string) =
                Record.PrettyPrint(template, deployments, ?title = title, useBorders = false)

        let tryGetDeploymentConfiguration (serviceName : string) (client : SubscriptionClient) = async {
            let! result = client.Compute.HostedServices.GetDetailedAsync serviceName |> Async.AwaitTaskCorrect |> Async.Catch
            match result with
            | Choice1Of2 service when service.Properties.ExtendedProperties |> isMBraceAsset ->
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
                if service.Properties.ExtendedProperties |> isMBraceAsset then 
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
                            |> Seq.map (fun i -> { Id = i.InstanceName ; Hostname = i.HostName ; VMSize = i.InstanceSize ; Status = i.InstanceStatus }) 
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
                [ "StorageAccountName", storageAccountName
                  "StorageConnectionString", storageConnectionString
                  "ServiceBusName", serviceBusNamespace
                  "ServiceBusConnectionString", serviceBusConnectionString ]
                @ (defaultExtendedProperties.Keys |> Seq.map(fun key -> key, defaultExtendedProperties.[key]) |> Seq.toList)
                |> dict

            logger.Logf LogLevel.Info "creating cloud service %s" serviceName
            let! _ = client.Compute.HostedServices.CreateAsync(HostedServiceCreateParameters(Location = region, ServiceName = serviceName, ExtendedProperties = extendedProperties))

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

        let deleteMBraceDeployment (logger : ISystemLogger) (serviceName:string) (client:SubscriptionClient) = async {
            let! (service : HostedServiceGetDetailedResponse) = client.Compute.HostedServices.GetDetailedAsync serviceName
            if service.Properties.ExtendedProperties |> isMBraceAsset then
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

/// Client object for managing MBrace Cloud Service deployments for user-suppplied Azure subscriptions
[<Sealed; AutoSerializable(false)>]
type DeploymentManager private (pubSettings : PubSettingsClient, defaultRegion : Region, _logger : ISystemLogger option, ?logLevel : LogLevel) =

    let logger = AttacheableLogger.Create(?logLevel = logLevel, makeAsynchronous = false)
    do _logger |> Option.iter(fun l -> ignore <| logger.AttachLogger l)

    let syncRoot = new obj()
    let mutable defaultRegion = defaultRegion
    let mutable pubSettings = pubSettings

    /// Attaches logger to the deployment manager instance
    member __.AttachLogger(l : ISystemLogger) = logger.AttachLogger l
    /// Gets or sets the default region used by the client instance
    member __.DefaultRegion
        with get () = defaultRegion
        and set reg = defaultRegion <- reg

    /// Lists all subscription names supplied in the current deployment manager instance
    member __.Subscriptions = pubSettings.Subscriptions |> Array.map (fun s -> s.Subscription.Name)
    /// Gets or sets the default subscription used by the deployment manager instance
    member __.DefaultSubscription 
        with get () = pubSettings.Default.Subscription.Name
        and set subId =
            lock syncRoot (fun () ->
                let sub = pubSettings.[subId]
                pubSettings <- { pubSettings with Default = sub })

    /// <summary>
    ///     Asynchronously starts deployment of MBrace cloud service with supplied parameters.
    /// </summary>
    /// <param name="vmSize">VM size used for deployment.</param>
    /// <param name="vmCount">VM instance count.</param>
    /// <param name="serviceName">Service name identifier. Defaults to auto-generated name.</param>
    /// <param name="subscriptionId">Subscription identifier for service deployment. Defaults to manager instance default subscription.</param>
    /// <param name="region">Region for service deployment. Defaults to manager instance default region.</param>
    /// <param name="mbraceVersion">MBrace version string used for .cspkg resolution. Defaults to current version.</param>
    /// <param name="storageAccount">Storage account name or connection string used by MBrace service. Defaults to self-allocated storage account.</param>
    /// <param name="serviceBusAccount">Service bus account name or connection string used by MBrace service. Defaults to self-allocation service bus account.</param>
    /// <param name="cloudServicePackage">Path or Uri to MBrace cloud service package to be deployed to Service. Defaults to .cspkg resolved from github.</param>
    /// <param name="serviceLabel">User-supplied service label.</param>
    member __.BeginDeployAsync(vmSize : VMSize, vmCount : int, ?serviceName : string, ?subscriptionId : string, ?region : Region, 
                                ?mbraceVersion : string, ?storageAccount : string, ?serviceBusAccount : string, ?cloudServicePackage : string, 
                                ?serviceLabel : string) = async {

        if vmCount < 1 then invalidOp "vmCount" "Must be positive value."
        let region = defaultArg region defaultRegion
        let client = pubSettings.GetClientByIdOrDefault(?id = subscriptionId)
        let serviceName = match serviceName with None -> generateResourceName() | Some sn -> sn
        do! Deployment.validateServiceName client serviceName

        logger.Logf LogLevel.Info "using vm size %s" vmSize
        let! packagePath, versionInfo = Deployment.downloadServicePackage logger vmSize mbraceVersion cloudServicePackage
        logger.Logf LogLevel.Info "using cluster name %s" serviceName

        let! storageAccountName, storageConnectionString = Storage.resolveStorageAccount logger region storageAccount client
        logger.Logf LogLevel.Info "using storage account name %A" storageAccountName
        let! _, serviceBusNamespace, serviceBusConnectionString = ServiceBus.resolveNamespaceInfo logger region serviceBusAccount client
        logger.Logf LogLevel.Info "using service bus account %A" serviceBusNamespace

        let config = Deployment.buildMBraceConfig serviceName vmCount storageConnectionString serviceBusConnectionString

        let clusterLabel = defaultArg serviceLabel (sprintf "MBrace cluster %A, package %s"  serviceName (defaultArg versionInfo "custom"))
        let! deployInfo = Deployment.prepareMBraceServiceDeployment logger serviceName clusterLabel region packagePath config storageAccountName storageConnectionString serviceBusNamespace serviceBusConnectionString client
        do! Deployment.beginDeploy DeploymentSlot.Production deployInfo client
        return new Configuration(storageConnectionString, serviceBusConnectionString)
    }

    /// <summary>
    ///     Starts deployment of MBrace cloud service with supplied parameters.
    /// </summary>
    /// <param name="vmSize">VM size used for deployment.</param>
    /// <param name="vmCount">VM instance count.</param>
    /// <param name="serviceName">Service name identifier. Defaults to auto-generated name.</param>
    /// <param name="subscriptionId">Subscription identifier for service deployment. Defaults to manager instance default subscription.</param>
    /// <param name="region">Region for service deployment. Defaults to manager instance default region.</param>
    /// <param name="mbraceVersion">MBrace version string used for .cspkg resolution. Defaults to current version.</param>
    /// <param name="storageAccount">Storage account name or connection string used by MBrace service. Defaults to self-allocated storage account.</param>
    /// <param name="serviceBusAccount">Service bus account name or connection string used by MBrace service. Defaults to self-allocation service bus account.</param>
    /// <param name="cloudServicePackage">Path or Uri to MBrace cloud service package to be deployed to Service. Defaults to .cspkg resolved from github.</param>
    /// <param name="serviceLabel">User-supplied service label.</param>
    member __.BeginDeploy(vmSize : VMSize, vmCount : int, ?serviceName : string, ?subscriptionId : string, ?region : Region, 
                                        ?mbraceVersion : string, ?storageAccount : string, ?serviceBusAccount : string, ?cloudServicePackage : string, ?serviceLabel : string) =
        __.BeginDeployAsync(vmSize, vmCount, ?serviceName = serviceName, ?subscriptionId = subscriptionId, ?region = region, ?mbraceVersion = mbraceVersion,
                                ?storageAccount = storageAccount, ?serviceBusAccount = serviceBusAccount, ?cloudServicePackage = cloudServicePackage, ?serviceLabel = serviceLabel)
        |> Async.RunSync

    /// <summary>
    ///     Asynchronously deletes deployment of given name.
    /// </summary>
    /// <param name="serviceName">Service name to be deleted.</param>
    /// <param name="subscriptionId">Subscription identifier from which to delete deployment. Defaults to manager instance subscription default.</param>
    member __.DeleteDeploymentAsync(serviceName : string, ?subscriptionId : string) = async { 
        let client = pubSettings.GetClientByIdOrDefault(?id = subscriptionId) 
        return! Deployment.deleteMBraceDeployment logger serviceName client
    }

    /// <summary>
    ///     Deletes deployment of given name.
    /// </summary>
    /// <param name="serviceName">Service name to be deleted.</param>
    /// <param name="subscriptionId">Subscription identifier from which to delete deployment. Defaults to manager instance subscription default.</param>
    member __.DeleteDeployment(serviceName : string, ?subscriptionId : string) =
        __.DeleteDeploymentAsync(serviceName, ?subscriptionId = subscriptionId)
        |> Async.RunSync

    /// <summary>
    ///     Asynchronously gets a printed report on deployments.
    /// </summary>
    /// <param name="subscriptionId">Subscription id to fetch deployments from. Defaults to manager instance subscription default.</param>
    member __.GetDeploymentsAsync(?subscriptionId : string) = async {
        let client = pubSettings.GetClientByIdOrDefault(?id = subscriptionId)
        let! deployments = Deployment.getRunningDeployments client
        return Deployment.DeploymentReporter.Report(deployments, title = sprintf "Subscription: %A" client.Subscription.Name)
    }

    /// <summary>
    ///     Prints a report on deployments to stdout.
    /// </summary>
    /// <param name="subscriptionId">Subscription id to fetch deployments from. Defaults to manager instance subscription default.</param>
    member __.ShowDeployments(?subscriptionId : string) =
        let info = __.GetDeploymentsAsync(?subscriptionId = subscriptionId) |> Async.RunSync
        Console.WriteLine(info)

    /// <summary>
    ///     Asynchronously gets the Azure cluster configuration assigned to the given
    ///     MBrace service deployment.
    /// </summary>
    /// <param name="serviceName">Service name to be looked up.</param>
    /// <param name="subscriptionId">Subscription id to fetch deployments from. Defaults to manager instance subscription default.</param>
    member __.GetConfigurationAsync(serviceName : string, ?subscriptionId : string) = async {
        let client = pubSettings.GetClientByIdOrDefault(?id = subscriptionId)
        let! result = Deployment.tryGetDeploymentConfiguration serviceName client
        match result with
        | Some config -> return config
        | None -> return invalidArg "serviceName" <| sprintf "Could not find service name '%s'." serviceName
    }

    /// <summary>
    ///     Gets the Azure cluster configuration assigned to the given
    ///     MBrace service deployment.
    /// </summary>
    /// <param name="serviceName">Service name to be looked up.</param>
    /// <param name="subscriptionId">Subscription id to fetch deployments from. Defaults to manager instance subscription default.</param>
    member __.GetConfiguration(serviceName : string, ?subscriptionId : string) =
        __.GetConfigurationAsync(serviceName, ?subscriptionId = subscriptionId)
        |> Async.RunSync

    /// <summary>
    ///     Creates a new subscription manager instance using supplied set of Azure subscriptions
    /// </summary>
    /// <param name="subscriptions">Subscriptions to manage.</param>
    /// <param name="defaultRegion">Default Azure region for deployments.</param>
    /// <param name="defaultSubscriptionId">Default subscription id used by the manager instance. Defaults to first subscription in inputs.</param>
    /// <param name="logger">System logger used by the manager instance. Defaults to no logging.</param>
    /// <param name="logLevel">Log level used by the manager instance. Defaults to Info.</param>
    static member Create(subscriptions : seq<Subscription>, defaultRegion : Region, ?defaultSubscriptionId : string, ?logger : ISystemLogger, ?logLevel : LogLevel) =
        let client = PubSettingsClient.Activate(subscriptions, ?defaultSubscriptionId = defaultSubscriptionId)
        new DeploymentManager(client, defaultRegion, logger, ?logLevel = logLevel)

    /// <summary>
    ///     Creates a new subscription manager instance using supplied set of Azure subscriptions
    /// </summary>
    /// <param name="publishSettings">Parsed PublishSettings record.</param>
    /// <param name="defaultRegion">Default Azure region for deployments.</param>
    /// <param name="defaultSubscriptionId">Default subscription id used by the manager instance. Defaults to first subscription in inputs.</param>
    /// <param name="logger">System logger used by the manager instance. Defaults to no logging.</param>
    /// <param name="logLevel">Log level used by the manager instance. Defaults to Info.</param>
    static member Create(publishSettings : PublishSettings, defaultRegion : Region, ?defaultSubscriptionId : string, ?logger : ISystemLogger, ?logLevel : LogLevel) =
        DeploymentManager.Create(publishSettings.Subscriptions, defaultRegion, ?logger = logger, ?defaultSubscriptionId = defaultSubscriptionId, ?logLevel = logLevel)

    /// <summary>
    ///     Creates a new subscription manager instance using supplied Azure subscription
    /// </summary>
    /// <param name="subscription">Subscription to manage.</param>
    /// <param name="defaultRegion">Default Azure region for deployments.</param>
    /// <param name="logger">System logger used by the manager instance. Defaults to no logging.</param>
    /// <param name="logLevel">Log level used by the manager instance. Defaults to Info.</param>
    static member Create(subscription : Subscription, defaultRegion : Region, ?logger : ISystemLogger, ?logLevel : LogLevel) =
        DeploymentManager.Create([subscription], defaultRegion, ?logger = logger, ?logLevel = logLevel)

    /// <summary>
    ///     Creates a new subscription manager instance using local Azure PublishSettings file.
    /// </summary>
    /// <param name="publishSettingsFile">Path to local PublishSettings file.</param>
    /// <param name="defaultRegion">Default Azure region for deployments.</param>
    /// <param name="defaultSubscriptionId">Default subscription id used by the manager instance. Defaults to first subscription in inputs.</param>
    /// <param name="logger">System logger used by the manager instance. Defaults to no logging.</param>
    /// <param name="logLevel">Log level used by the manager instance. Defaults to Info.</param>
    static member FromPublishSettingsFile(publishSettingsFile : string, defaultRegion : Region, ?defaultSubscriptionId : string, ?logger : ISystemLogger, ?logLevel : LogLevel) =
        let pubSettings = PublishSettings.ParseFile publishSettingsFile
        DeploymentManager.Create(pubSettings.Subscriptions, defaultRegion, ?defaultSubscriptionId = defaultSubscriptionId, ?logger = logger, ?logLevel = logLevel)