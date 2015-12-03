namespace MBrace.Azure.Management

open System
open System.IO
open System.Xml.Linq

open MBrace.Azure
open MBrace.Azure.Runtime

type internal OAttribute = System.Runtime.InteropServices.OptionalAttribute
type internal DAttribute = System.Runtime.InteropServices.DefaultParameterValueAttribute

/// Azure Region Identifier
[<Sealed; AutoSerializable(true); StructuredFormatDisplay("{Id}")>]
type Region private (regionId : string) = 
    /// Azure region string identifier
    member __.Id = regionId
    /// Creates a custom Azure region definition
    static member Define(regionId : string) = Region regionId
    static member South_Central_US  = Region "South Central US"
    static member West_US           = Region "West US"
    static member Central_US        = Region "Central US"
    static member East_US           = Region "East US"
    static member East_US_2         = Region "East US 2"
    static member North_Europe      = Region "North Europe"
    static member West_Europe       = Region "West Europe"
    static member Southeast_Asia    = Region "Southeast Asia"
    static member East_Asia         = Region "East Asia"

    override __.GetHashCode() = hash regionId
    override __.Equals(other:obj) =
        match other with :? Region as r -> r.Id = regionId | _ -> false

    override __.ToString() = regionId

/// Azure VM size identifier
[<Sealed; AutoSerializable(true); StructuredFormatDisplay("{Id}")>]
type VMSize private (vmId : string) =
    /// Azure VM string identifier
    member __.Id = vmId
    /// Creates a custom Azure VM size definition
    static member Define(vmId : string) = VMSize vmId

    static member A10               = VMSize "A10"
    static member A11               = VMSize "A11"
    static member A5                = VMSize "A5"
    static member A6                = VMSize "A6"
    static member A7                = VMSize "A7"
    static member A8                = VMSize "A8"
    static member A9                = VMSize "A9"
    static member A4                = VMSize "ExtraLarge"
    static member A0                = VMSize "ExtraSmall"
    static member A3                = VMSize "Large"
    static member A2                = VMSize "Medium"
    static member A1                = VMSize "Small"
    static member Extra_Large       = VMSize "ExtraLarge"
    static member Large             = VMSize "Large"
    static member Medium            = VMSize "Medium"
    static member Small             = VMSize "Small"
    static member Extra_Small       = VMSize "ExtraSmall"
    static member Standard_D1       = VMSize "Standard_D1"
    static member Standard_D11      = VMSize "Standard_D11"
    static member Standard_D11_v2   = VMSize "Standard_D11_v2"
    static member Standard_D12      = VMSize "Standard_D12"
    static member Standard_D12_v2   = VMSize "Standard_D12_v2"
    static member Standard_D13      = VMSize "Standard_D13"
    static member Standard_D13_v2   = VMSize "Standard_D13_v2"
    static member Standard_D14      = VMSize "Standard_D14"
    static member Standard_D14_v2   = VMSize "Standard_D14_v2"
    static member Standard_D1_v2    = VMSize "Standard_D1_v2"
    static member Standard_D2       = VMSize "Standard_D2"
    static member Standard_D2_v2    = VMSize "Standard_D2_v2"
    static member Standard_D3       = VMSize "Standard_D3"
    static member Standard_D3_v2    = VMSize "Standard_D3_v2"
    static member Standard_D4       = VMSize "Standard_D4"
    static member Standard_D4_v2    = VMSize "Standard_D4_v2"
    static member Standard_D5_v2    = VMSize "Standard_D5_v2"

    override __.GetHashCode() = hash vmId
    override __.Equals(other:obj) =
        match other with :? VMSize as r -> r.Id = vmId | _ -> false

    override __.ToString() = vmId


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
[<NoEquality; NoComparison; AutoSerializable(false);  StructuredFormatDisplay("{SubscriptionNames}")>]
type PublishSettings =
    {
        /// Set of Azure subscriptions defined in PubSettings
        Subscriptions : Subscription []
    }

    /// Look up subscription by id or partial name
    member ps.GetSubscriptionById (subscriptionId : string) =
        match ps.Subscriptions |> Array.filter (fun s -> s.Id = subscriptionId || s.Name.Contains subscriptionId) with
        | [||] -> invalidArg "subscriptionId" <| sprintf "Could not find subscription id %A" subscriptionId
        | [|sub|] -> sub
        | subs -> 
            let subs = subs |> Seq.map (fun s -> sprintf "%A" s.Name) |> String.concat ", "
            invalidArg "subscription" <| sprintf "Ambiguous matches for subscription id %A: %s" subscriptionId subs

    /// Look up subscription by index
    member ps.Item (index:int) = ps.Subscriptions.[index]

    /// Gets the names of all contained subscriptions
    member ps.SubscriptionNames = ps.Subscriptions |> Array.map (fun s -> s.Name)

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

/// Azure Storage Account descriptor
[<Sealed; AutoSerializable(false)>]
type StorageAccount internal (inner : AzureStorageAccount) =
    member __.AccountName = inner.AccountName
    member __.AccountKey = inner.AccountKey
    member __.ConnectionString = inner.ConnectionString
    member internal __.Inner = inner
    /// <summary>
    ///     Parses storage account using provided connection string
    /// </summary>
    /// <param name="connectionString">Azure storage connection string</param>
    static member FromConnectionString(connectionString : string) =
        new StorageAccount(AzureStorageAccount.FromConnectionString connectionString)

/// Azure Service Bus Account descriptor
[<Sealed; AutoSerializable(false)>]
type ServiceBusAccount internal (inner : AzureServiceBusAccount) =
    member __.AccountName = inner.AccountName
    member __.AccountKey = inner.AccountKey
    member __.ConnectionString = inner.ConnectionString
    member internal __.Inner = inner
    /// <summary>
    ///     Parses service bus account using provided connection string
    /// </summary>
    /// <param name="connectionString">Azure service bus connection string</param>
    static member FromConnectionString(connectionString : string) =
        new ServiceBusAccount(AzureServiceBusAccount.FromConnectionString connectionString)


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
type DeploymentStatus =
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
        | Provisioning pct -> sprintf "Provisioning %2.1f%%" (pct * 100.)
        | Ready -> "Ready"
        | RunningTransitioning -> "RunningTransitioning"
        | SuspendedTransitioning -> "SuspendedTransitioning"
        | Suspending -> "Suspending"
        | Suspended -> "Suspended"
        | Deleting -> "Deleting"
        | Unknown -> "Unknown"

/// Deployment info record
type DeploymentInfo =
    {
        /// Deployment Name
        Name : string
        /// Deployment creation time
        CreatedTime : DateTimeOffset
        /// Last cluster modification time
        LastModified : DateTimeOffset
        /// Cloud Service status
        ServiceStatus : string
        /// Service deployment label
        Label : string
        /// Deployment status
        DeploymentState : DeploymentStatus
        /// Azure Storage account used by MBrace deployment
        StorageAccount : StorageAccount
        /// Azure Service Bus account by MBrace deployment
        ServiceBusAccount : ServiceBusAccount
        /// VM Instance information
        VMInstances : VMInstance []
        /// Deployment Region
        Region : Region
    }