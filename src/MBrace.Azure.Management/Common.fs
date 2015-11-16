namespace MBrace.Azure.Management

open System
open System.Collections.Generic
open System.Security.Cryptography.X509Certificates

open Microsoft.Azure
open Microsoft.WindowsAzure.Management
open Microsoft.WindowsAzure.Management.Compute
open Microsoft.WindowsAzure.Management.Storage
open Microsoft.WindowsAzure.Management.ServiceBus

[<AutoOpen>]
module internal Common =

    let defaultMBraceVersion = System.AssemblyVersionInformation.ReleaseTag

    let resourcePrefix = "mbrace"
    let generateResourceName() = resourcePrefix + Guid.NewGuid().ToString("N").[..7]

    let getPackageUrl mbraceNugetVersionTag (vmSize : VMSize) = 
        sprintf "https://github.com/mbraceproject/MBrace.Azure/releases/download/%s/MBrace.Azure.CloudService-%s.cspkg" 
                mbraceNugetVersionTag vmSize.Id

    let defaultExtendedProperties = dict [ "IsMBraceAsset", "true"]
    let isMBraceAsset (extendedProperties:IDictionary<string, string>) = extendedProperties.ContainsKey "IsMBraceAsset"

    /// Represents an Azure client instance for specific subscription
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
            {   
                Subscription = subscription
                Credentials = cred
                Storage = new StorageManagementClient(cred)
                ServiceBus = new ServiceBusManagementClient(cred)
                Compute = new ComputeManagementClient(cred)
                Management = new ManagementClient(cred) 
            }