
namespace MBrace.Azure


[<Class>]
/// The regions in which Azure clusters can be created
type Regions = 
    static member ``South Central US`` : string
    static member ``West US`` : string
    static member ``Central US`` : string
    static member ``East US`` : string
    static member ``East US 2`` : string
    static member ``North Europe`` : string
    static member ``West Europe`` : string
    static member ``Southeast Asia`` : string
    static member ``East Asia`` : string

[<Class>]
/// The VM sizes for Azure clusters 
type VMSizes = 
    static member A10 : string
    static member A11 : string
    static member A5 : string
    static member A6 : string
    static member A7 : string
    static member A8 : string
    static member A9 : string
    /// Same as Extra Large
    static member A4 : string
    /// Same as Extra Small
    static member A0 : string
    /// Same as Large
    static member A3 : string
    /// Same as Medium
    static member A2 : string
    /// Same as Small
    static member A1 : string
    static member ``Extra Large`` : string
    static member Large : string
    static member Medium : string
    static member Small : string
    static member ``ExtraSmall`` : string
    static member ``Standard D1`` : string
    static member ``Standard D11`` : string
    static member ``Standard D11 v2`` : string
    static member ``Standard D12`` : string
    static member ``Standard D12 v2`` : string
    static member ``Standard D13`` : string
    static member ``Standard D13 v2`` : string
    static member ``Standard D14`` : string
    static member ``Standard D14 v2`` : string
    static member ``Standard D1 v2`` : string
    static member ``Standard D2`` : string
    static member ``Standard D2 v2`` : string
    static member ``Standard D3`` : string
    static member ``Standard D3 v2`` : string
    static member ``Standard D4`` : string
    static member ``Standard D4 v2`` : string
    static member ``Standard D5 v2`` : string


[<Class>]
type Management = 

    /// <summary>Provision an MBrace cluster in the subscription from the pubsettings file</summary>
    /// <param name="pubSettingsFile">The path to the pubsettings file. Download from https://manage.windowsazure.com/publishsettings</param>
    /// <param name="region">The Azure region in which to create the cluster. Choose from Regions.*</param>
    /// <param name="ClusterName">The name of the cluster. Defaults to an auto-generated cluster name.</param>
    /// <param name="Subscription">The subscription to use. Defaults to the first subscription available in the publish settings.</param>
    /// <param name="MBraceVersion">The MBrace software version id to use. Ignored if using an explicit package. Defaults to this version of MBrace.Azure.</param>
    /// <param name="VMCount">The number of virtual machines to allocate in the cluster. Defaults to 2.</param>
    /// <param name="VMSize">The size of virtual machines to allocate in the cluster. Use one of VMSizes.*. Defaults to Large.</param>
    /// <param name="StorageAccount">The name of the storage ccount to use. Defaults to reusing a suitable existing account if available, otherwise creates a new one.</param>
    /// <param name="CloudServicePackage">An explicit cloud service package to use. If not specified, will use a basic MBrace Cloud Service.</param>
    /// <param name="ClusterLabel">The label to give the deployment of the cloud service. Defaults to a label providing details on this cluster.</param>
    static member CreateCluster : pubSettingsFile : string * region : string * ?ClusterName : string *  ?Subscription : string *  ?MBraceVersion : string * ?VMCount: int * ?StorageAccount: string * ?VMSize: string * ?CloudServicePackage : string * ?ClusterLabel : string -> Configuration

    /// <summary>Delete the given cluster from the subscription from the pubsettings file</summary>
    /// <param name="pubSettingsFile">The path to the pubsettings file. Download from https://manage.windowsazure.com/publishsettings</param>
    /// <param name="clusterName">The name of the cluster</param>
    /// <param name="Subscription">The subscription to use</param>
    static member DeleteCluster : pubSettingsFile : string * clusterName : string * ?Subscription : string -> unit

    /// <summaryGet a string representation of each of the MBrace clusters in the subscription from the pubsettings file</summary>
    /// <param name="pubSettingsFile">The path to the pubsettings file. Download from https://manage.windowsazure.com/publishsettings</param>
    /// <param name="clusterName">The name of the cluster</param>
    /// <param name="Subscription">The subscription to use</param>
    static member GetClusters : pubSettingsFile : string * ?Subscription : string  -> string list

