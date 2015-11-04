
namespace MBrace.Azure

open Microsoft.Azure
open Microsoft.WindowsAzure.Management
open Microsoft.WindowsAzure.Management.Compute
open Microsoft.WindowsAzure.Management.Compute.Models
open Microsoft.WindowsAzure.Management.Storage
open Microsoft.WindowsAzure.Management.Storage.Models
open Microsoft.WindowsAzure.Management.ServiceBus
open System.Security.Cryptography.X509Certificates
open System
open System.IO
open System.Xml.Linq
open System.Collections.Generic

[<AutoOpen>]
module private Details = 

    let urlForPackage mbraceNugetVersionTag vmSize = 
        sprintf "https://github.com/mbraceproject/MBrace.Azure/releases/download/%s/MBrace.Azure.CloudService-%s.cspkg" mbraceNugetVersionTag vmSize
        //sprintf "https://github.com/mbraceproject/bits/raw/master/%s/MBrace.Azure.CloudService-%s.cspkg" mbraceVersion vmSize

    type Result<'TSuccess, 'TMessage> = 
        | Ok of 'TSuccess * 'TMessage list
        | Bad of 'TMessage list

    /// Basic combinators and operators for error handling.
    [<AutoOpen>]
    module Trial =
        let ok x = Ok(x, [])
        let info msg = printfn "%s" msg; Ok((),[msg])
        let fail msg = printfn "Error: %s" msg; Bad([ msg ])
        let protect f = (fun x -> try f x with e -> fail e.Message)

        let returnOrFail result = 
            match result with
            | Ok(x, _msgs) -> x
            | Bad(msgs) -> 
                let msg = msgs |> Seq.map (sprintf "%O") |> String.concat (Environment.NewLine + "   ")
                failwith msg

        let mergeMessages msgs result = 
            match result with
            | Ok(x, msgs2) -> Ok(x, msgs @ msgs2)
            | Bad(msgs2) ->  Bad(msgs @ msgs2)

        let bind f result = 
            match result with
            | Ok(x, msgs2) -> f x |> mergeMessages msgs2
            | Bad(errs) ->  Bad errs

        /// Builder type for error handling computation expressions.
        type TrialBuilder() = 
            member __.Zero() = ok()
            member __.Bind(m, f) = bind f m
            member __.Return(x) = ok x
            member __.ReturnFrom(x) = x
            member __.Combine (a, b) = bind (protect b) a
            member __.Delay f = protect f
            member __.Run f = protect f ()
            member __.TryWith (body, handler) = try body() with e -> handler e
            member __.TryFinally (body, compensation) = try body() finally compensation()
            member x.Using(d:#IDisposable, body) =
                x.TryFinally ((fun () -> body d), fun () -> match d with null -> () | d -> d.Dispose())
            member x.While (guard, body) =
                if not (guard ()) then x.Zero()
                else bind (fun () -> x.While(guard, body)) (body())

        let trial = TrialBuilder()

    let resourcePrefix = "mbrace"
    let generateResourceName() = resourcePrefix + (Guid.NewGuid().ToString() |> Seq.take 8 |> Seq.toArray |> (fun c -> String(c)))
    let defaultExtendedProperties = dict [ "IsMBraceAsset", "true"]
    let isMBraceAsset (extendedProperties:IDictionary<string, string>) = extendedProperties.ContainsKey "IsMBraceAsset"

    //type PubSettings = XmlProvider< """<?xml version="1.0" encoding="utf-8"?><PublishData><PublishProfile SchemaVersion="2.0" PublishMethod="AzureServiceManagementAPI"><Subscription ServiceManagementUrl="https://management.core.windows.net" Id="de495429-2562-5242-81c8-54e236a5db4d" Name="Your First Subscription" ManagementCertificate="FIODSFSDIUS" /><Subscription ServiceManagementUrl="https://management.core.windows.net" Id="e156f021-460a-42fb-8a52-2878fbf83a25" Name="Second Subscription" ManagementCertificate="BLAH"/></PublishProfile></PublishData> """>
    type Subscription = 
        { Name : string; Id : string;  ManagementCertificate: string; ServiceManagementUrl:string }
        static member LoadX(doc:XElement) = 
            let name = doc.Attribute(XName.op_Implicit "Name").Value
            let id = doc.Attribute(XName.op_Implicit "Id").Value
            let mc = doc.Attribute(XName.op_Implicit "ManagementCertificate").Value
            let sm = doc.Attribute(XName.op_Implicit "ServiceManagementUrl").Value
            { Name = name;
              Id = id;
              ManagementCertificate = mc
              ServiceManagementUrl = sm }

    type PublishProfile = 
        { Subscriptions: Subscription [] }
        static member LoadX(doc:XElement) = 
            let subs = [| for s in doc.Elements(XName.op_Implicit "Subscription") -> Subscription.LoadX s |]
            { Subscriptions = subs }

    type PubSettings = 
        { PublishProfile: PublishProfile }
        static member LoadX(doc:XDocument) = 
                let publishDataXml = doc.Element(XName.op_Implicit "PublishData")
                let publishProfileXml = publishDataXml.Element(XName.op_Implicit "PublishProfile")
                { PublishProfile =  PublishProfile.LoadX publishProfileXml } 
        static member Parse(xml:string) = PubSettings.LoadX (XDocument.Parse xml)
    //PubSettings.Parse """<?xml version="1.0" encoding="utf-8"?><PublishData><PublishProfile SchemaVersion="2.0" PublishMethod="AzureServiceManagementAPI"><Subscription     ServiceManagementUrl="https://management.core.windows.net" Id="de495429-2562-5242-81c8-54e236a5db4d" Name="Your First Subscription" ManagementCertificate="FIODSFSDIUS" /><Subscription ServiceManagementUrl="https://management.core.windows.net" Id="e156f021-460a-42fb-8a52-2878fbf83a25" Name="Second Subscription" ManagementCertificate="BLAH"/></PublishProfile></PublishData> """


    type AzureClient =
        { Subscription : string
          Credentials : CertificateCloudCredentials
          Storage : StorageManagementClient
          ServiceBus : ServiceBusManagementClient
          Compute : ComputeManagementClient
          Management : ManagementClient }
        static member OfCredentials (subscription:Subscription) credentials =
            { Subscription = subscription.Name
              Credentials = credentials
              Storage = new StorageManagementClient(credentials)
              ServiceBus = new ServiceBusManagementClient(credentials)
              Compute = new ComputeManagementClient(credentials)
              Management = new ManagementClient(credentials) }

    module Credentials =
        let asCredentials (subscription:Subscription) =
            new CertificateCloudCredentials(
                subscription.Id,
                new X509Certificate2(
                    subscription.ManagementCertificate |> Convert.FromBase64String))

        let getSubscriptions (pubSettingsFilePath:string) =
            let pubSettings = PubSettings.Parse (File.ReadAllText pubSettingsFilePath)
            pubSettings.PublishProfile.Subscriptions

        let connectToAzure subscriptionOpt pubSettingsFilePath =
          trial {
            let subscriptions = getSubscriptions pubSettingsFilePath
            if subscriptions.Length = 0 then 
                return! fail (sprintf "no Azure subscriptions found in pubsettings file %s" pubSettingsFilePath)
            let subscription = defaultArg subscriptionOpt subscriptions.[0].Name

            match subscriptions |> Seq.tryFind(fun s -> s.Name = subscription) with 
            | None -> return! fail (sprintf "couldn't find subscription %s in %s" subscription pubSettingsFilePath)
            | Some sub -> return sub |> asCredentials |> AzureClient.OfCredentials sub 
        }

    module Infrastructure =
        let listRegions (client:AzureClient) =
            let requiredServices = [ "Compute"; "Storage" ]
            let rolesForClient = set(client.Management.RoleSizes.List() |> Seq.map(fun r -> r.Name))
            client.Management.Locations.List()
            |> Seq.filter(fun location -> requiredServices |> List.forall(location.AvailableServices.Contains))
            |> Seq.map(fun location ->
                let rolesInLocation = set location.ComputeCapabilities.WebWorkerRoleSizes
                location.Name, rolesForClient |> Set.intersect rolesInLocation |> Seq.toList)
            |> Seq.toList


    module Storage =
        open Microsoft.WindowsAzure.Storage

        let listStorageAccounts region (client:AzureClient) =
            [ for account in client.Storage.StorageAccounts.List() do
                let hasLocationData, storageAccountLocation = account.ExtendedProperties.TryGetValue "ResourceLocation"
                if hasLocationData && storageAccountLocation = region then 
                   yield account ]

        let tryFindMBraceStorage region (client:AzureClient) =
            client
            |> listStorageAccounts region
            |> List.filter(fun account -> account.ExtendedProperties |> isMBraceAsset)
            |> List.map(fun account -> account.Name)
            |> function [] -> None | h::_ -> Some h

        let rec createMBraceStorageAccount region (client:AzureClient) =
            trial {
                let accountName = generateResourceName()
                let availability = client.Storage.StorageAccounts.CheckNameAvailability accountName 
                if not availability.IsAvailable then return! createMBraceStorageAccount region client
                else
                    let result = client.Storage.StorageAccounts.Create(StorageAccountCreateParameters(Name = accountName, AccountType = "Standard_LRS", Location = region, ExtendedProperties = defaultExtendedProperties))
                    if result.Status = OperationStatus.Failed then 
                        return! fail result.Error.Message
                    else 
                        do! info (sprintf "Created new storage account %s" accountName)
                        return accountName }

        let getConnectionString accountName (client:AzureClient) =
            trial { 
                let keys = client.Storage.StorageAccounts.GetKeys accountName
                return sprintf """DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s""" accountName keys.PrimaryKey
            }

        let getDeploymentContainer connectionString =
            let blobClient =
                let account = CloudStorageAccount.Parse connectionString
                account.CreateCloudBlobClient()
            blobClient.GetContainerReference "deployments"

        let configureMBraceStorageAccount connectionString = 
            let container = getDeploymentContainer connectionString
            container.CreateIfNotExists()

        let getDefaultMBraceStorageAccountName region client =
            trial {
                match tryFindMBraceStorage region client with
                | Some storage -> 
                    do! info (sprintf "Reusing existing storage account %s" storage)
                    return storage
                | None -> 
                    return! createMBraceStorageAccount region client
            }

        let getMBraceStorageConnectionString accountName (client:AzureClient) =
            trial {
                let! connectionString = getConnectionString accountName client 
                configureMBraceStorageAccount connectionString |> ignore
                return connectionString
            }

    module ServiceBus =
        let rec waitUntilState checkState ns (client:AzureClient) =
            let status = try (client.ServiceBus.Namespaces.Get ns).Namespace.Status with ex -> ""
            if not (checkState status) then
                Async.Sleep 2000 |> Async.RunSynchronously
                waitUntilState checkState ns client

        let rec createNamespace region (client:AzureClient) =
            trial {
                let namespaceName = generateResourceName()
                do! info (sprintf "checking availability of service bus namespace %s" namespaceName)
                let availability = client.ServiceBus.Namespaces.CheckAvailability namespaceName
                if not availability.IsAvailable then 
                    return! createNamespace region client
                else
                    do! info (sprintf "creating service bus namespace %s" namespaceName)
                    let result = client.ServiceBus.Namespaces.Create(namespaceName, region) 

                    client |> waitUntilState ((=) "Active") namespaceName

                    if result.StatusCode <> Net.HttpStatusCode.OK then 
                        return! fail(sprintf "Failed to create service bus: %O" result.StatusCode)
                    else 
                        do! info (sprintf "Created new default MBrace Service Bus namespace %s" namespaceName)
                        return namespaceName }

        let listNamespaces region (client:AzureClient) =
            [ for account in client.ServiceBus.Namespaces.List() do 
                 if account.Region = region then 
                    yield account ]

        let tryFindMBraceNamespace region (client:AzureClient) =
            client
            |> listNamespaces region
            |> List.filter(fun ns -> ns.Name.StartsWith resourcePrefix)
            |> List.map(fun ns -> ns.Name)
            |> function [] -> None | h :: _ -> Some h

        let getConnectionString namespaceName (client:AzureClient) =
            trial {
                let rule = client.ServiceBus.Namespaces.ListAuthorizationRules namespaceName |> Seq.find(fun rule -> rule.KeyName = "RootManageSharedAccessKey")
                return sprintf """Endpoint=sb://%s.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=%s""" namespaceName rule.PrimaryKey
            } 

        let findOrCreateMBraceNamespace region client = 
            trial {
                match tryFindMBraceNamespace region client with
                | Some ns -> 
                    do! info (sprintf "Reusing existing Service Bus namespace %s" ns )
                    return ns
                | None -> return! createNamespace region client
            }

        let getDefaultMBraceNamespace region client =
            trial {
                let! ns = findOrCreateMBraceNamespace region client
                let! connectionString = getConnectionString ns client
                return ns, connectionString 
            }

        let deleteNamespace namespaceName (client:AzureClient) =
            trial {
                let _response = client.ServiceBus.Namespaces.Delete namespaceName
                client |> waitUntilState ((<>) "Removing") namespaceName
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

        let validateClusterName (client:AzureClient) clusterName =
            trial { 
                let result = client.Compute.HostedServices.CheckNameAvailability clusterName
                if not result.IsAvailable then return! fail result.Reason
            }

        let getConnectionStrings clusterName (client:AzureClient) =
            let service = client.Compute.HostedServices.GetDetailed clusterName
            let storageConnectionString = service.Properties.ExtendedProperties.["StorageAccountConnectionString"]
            let serviceBusConnectionString = service.Properties.ExtendedProperties.["ServiceBusConnectionString"]
            let serviceBusNamespace = service.Properties.ExtendedProperties.["ServiceBusName"]
            storageConnectionString, serviceBusConnectionString, serviceBusNamespace

        let listRunningMBraceClusters (client:AzureClient) =
            [ for service in client.Compute.HostedServices.List() do
                 if service.Properties.ExtendedProperties |> isMBraceAsset then 
                   let deployment =
                        try client.Compute.Deployments.GetBySlot(service.ServiceName, DeploymentSlot.Production) |> Some
                        with _ -> None
                   let info = 
                       { Name = service.ServiceName
                         CreatedTime = service.Properties.DateCreated
                         ServiceStatus = service.Properties.Status.ToString()
                         DeploymentStatus = deployment |> Option.map(fun deployment -> deployment.Status.ToString())
                         Nodes =
                            deployment
                            |> Option.map(fun deployment ->
                                deployment.RoleInstances
                                |> Seq.map(fun instance -> { Size = instance.InstanceSize; Status = instance.InstanceStatus })
                                |> Seq.toList)
                            |> defaultArg <| [] } 
                   yield info  |> sprintf "%+A" ]

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

        let createMBraceCluster(clusterName, clusterLabel, region, packagePath, config, storageAccountName, storageConnectionString, serviceBusNamespace, serviceBusConnectionString) (client:AzureClient) =
          trial {
            do! validateClusterName client clusterName 

            let extendedProperties =
                [ "StorageAccountName", storageAccountName
                  "StorageAccountConnectionString", storageConnectionString
                  "ServiceBusName", serviceBusNamespace
                  "ServiceBusConnectionString", serviceBusConnectionString ]
                @ (defaultExtendedProperties.Keys |> Seq.map(fun key -> key, defaultExtendedProperties.[key]) |> Seq.toList)
                |> dict

            do! info (sprintf "creating cloud service %s" clusterName)
            client.Compute.HostedServices.Create(HostedServiceCreateParameters(Location = region, ServiceName = clusterName, ExtendedProperties = extendedProperties)) |> ignore

            let container = Storage.getDeploymentContainer storageConnectionString
            let packageBlob = packagePath |> Path.GetFileName |> container.GetBlockBlobReference
            let blobSizesDoNotMatch() =
                packageBlob.FetchAttributes()
                packageBlob.Properties.Length <> FileInfo(packagePath).Length

            if (not (packageBlob.Exists()) || blobSizesDoNotMatch()) then
                do! info (sprintf "uploading package %s" packagePath)
                packageBlob.UploadFromFile(packagePath, FileMode.Open)
        
            do! info (sprintf "scheduling cluster creation:\n  cluster %s\n  package uri %s\n  config %s" clusterName (packageBlob.Uri.ToString()) config)
            let createOp = 
                client.Compute.Deployments.BeginCreating(
                    clusterName,
                    DeploymentSlot.Production,
                    DeploymentCreateParameters(
                        Label = clusterLabel, 
                        Name = clusterName,
                        PackageUri = packageBlob.Uri,
                        Configuration = config,
                        StartDeployment = Nullable true,
                        TreatWarningsAsError = Nullable true))

            if createOp.StatusCode <> Net.HttpStatusCode.Accepted then 
                return! fail "error: HTTP request for creation operation was not accepted"
          }

        let deleteMBraceCluster clusterName (client:AzureClient) =
            trial {
                let service = client.Compute.HostedServices.GetDetailed clusterName
                if service.Properties.ExtendedProperties |> isMBraceAsset then
                    do! info (sprintf "deleting cluster %s" clusterName)
                    let deleteOp = client.Compute.Deployments.DeleteByName(clusterName, clusterName, true)
                    if deleteOp.Status <> OperationStatus.Succeeded then return! fail deleteOp.Error.Message
                    let deleteOp = client.Compute.HostedServices.Delete clusterName
                    if deleteOp.StatusCode <> Net.HttpStatusCode.OK then return! fail(deleteOp.StatusCode.ToString()) 
                else
                    do! info (sprintf "No MBrace cluster called %s found" clusterName)
            }



type Regions() = 
    static member South_Central_US = "South Central US"
    static member West US = "West US"
    static member Central_US = "Central US"
    static member East_US = "East US"
    static member East_US = "East US 2"
    static member North_Europe = "North Europe"
    static member West_Europe = "West Europe"
    static member Southeast_Asia = "Southeast Asia"
    static member East_Asia = "East Asia"



type VMSizes() = 
    static member A10 = "A10"
    static member A11 = "A11"
    static member A5 = "A5"
    static member A6 = "A6"
    static member A7 = "A7"
    static member A8 = "A8"
    static member A9 = "A9"
    static member A4 = "ExtraLarge" // A4
    static member A0 = "ExtraSmall" // A0
    static member A3 = "Large" // A3
    static member A2 = "Medium" // A2
    static member A1 = "Small" // A1
    static member Extra_Large = "ExtraLarge" // A4
    static member Large = "Large" // A3
    static member Medium = "Medium" // A2
    static member Small = "Small" // A1
    static member Extra_Small = "ExtraSmall" // A0
    static member Standard_D1 = "Standard_D1"
    static member Standard_D11 = "Standard_D11"
    static member Standard_D11_v2 = "Standard_D11_v2"
    static member Standard_D12 = "Standard_D12"
    static member Standard_D12_v2 = "Standard_D12_v2"
    static member Standard_D13 = "Standard_D13"
    static member Standard_D13_v2 = "Standard_D13_v2"
    static member Standard_D14 = "Standard_D14"
    static member Standard_D14_v2 = "Standard_D14_v2"
    static member Standard_D1_v2 = "Standard_D1_v2"
    static member Standard_D2 = "Standard_D2"
    static member Standard_D2_v2 = "Standard_D2_v2"
    static member Standard_D3 = "Standard_D3"
    static member Standard_D3_v2 = "Standard_D3_v2";
    static member Standard_D4 = "Standard_D4"
    static member Standard_D4_v2 = "Standard_D4_v2"
    static member Standard_D5_v2 = "Standard_D5_v2"

type Management() = 
    static let defaultMBraceVersion = 
        try 
            let attr = System.Reflection.Assembly.GetExecutingAssembly().GetCustomAttributes(typeof<System.Reflection.AssemblyInformationalVersionAttribute>,false)
            match attr  with
            | [| :? System.Reflection.AssemblyInformationalVersionAttribute as a |] -> a.InformationalVersion
            | _ -> System.AssemblyVersionInformation.Version
        with _ -> System.AssemblyVersionInformation.Version

    static member CreateCluster(pubSettingsFile, region, ?ClusterName, ?Subscription, ?MBraceVersion, ?VMCount, ?StorageAccount, ?VMSize, ?CloudServicePackage, ?ClusterLabel) =
       trial { 
           let vmSize = defaultArg VMSize VMSizes.Large
           do! info (sprintf "using vm size %s" vmSize)
           let! packagePath, infoString = 
             trial { 
               match CloudServicePackage with 
               | Some uriOrPath when System.Uri(uriOrPath).IsFile ->
                   let path = System.Uri(uriOrPath).LocalPath
                   do! info (sprintf "using cloud service package from %s" path)
                   return path, "custom"
               | _ -> 
                   let mbraceVersion = defaultArg MBraceVersion defaultMBraceVersion
                   do! info (sprintf "using MBrace version tag %s" mbraceVersion)
                   let uri = defaultArg CloudServicePackage (urlForPackage mbraceVersion vmSize)
                   use wc = new System.Net.WebClient() 
                   let tmp = System.IO.Path.GetTempFileName() 
                   do! info (sprintf "downloading cloud service package from %s" uri)
                   wc.DownloadFile(uri, tmp)
                   return tmp, mbraceVersion
            }
           let! client = Credentials.connectToAzure Subscription pubSettingsFile 
           let clusterName = defaultArg ClusterName (generateResourceName())
           do! info (sprintf "using cluster name %s" clusterName)
           let! storageAccountName = 
             trial {
               match StorageAccount with 
               | None -> return! Storage.getDefaultMBraceStorageAccountName region client
               | Some s -> return s
             }
           let! storageConnectionString = client |> Storage.getMBraceStorageConnectionString storageAccountName 
           do! info (sprintf "using storage account name %s" storageAccountName)
           do! info (sprintf "using storage connection string %s..." storageConnectionString.[0..20])
           let! serviceBusNamespace, serviceBusConnectionString = client |> ServiceBus.getDefaultMBraceNamespace region
           do! info (sprintf "using service bus namespace %s" serviceBusNamespace)
           do! info (sprintf "using service bus connection string %s..." serviceBusConnectionString.[0..20])

           let numInstances = defaultArg VMCount 2
           let config = Clusters.buildMBraceConfig clusterName numInstances storageConnectionString serviceBusConnectionString
    

           let clusterLabel = defaultArg ClusterLabel (sprintf "MBrace cluster %s, package %s"  clusterName infoString)
           do! Clusters.createMBraceCluster(clusterName, clusterLabel, region, packagePath, config, storageAccountName, storageConnectionString, serviceBusNamespace, serviceBusConnectionString) client

           let config = Configuration(storageConnectionString, serviceBusConnectionString)

           return config
        }  |> Trial.returnOrFail

    static member DeleteCluster(pubSettingsFile, clusterName, ?Subscription) =
       trial { 
           let! client = Credentials.connectToAzure Subscription pubSettingsFile 
           return! Clusters.deleteMBraceCluster clusterName client

        }  |> Trial.returnOrFail

    static member GetClusters(pubSettingsFile, ?Subscription) =
       trial { 
           let! client = Credentials.connectToAzure Subscription pubSettingsFile 
           let response = Clusters.listRunningMBraceClusters client
           return response

        }  |> Trial.returnOrFail


