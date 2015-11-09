
namespace MBrace.Azure

/// Represents an Azure Region.
type Region = Region of Name:string

[<Class>]
/// The regions in which Azure clusters can be created
type Regions = 
    static member South_Central_US : Region
    static member West_US : Region
    static member Central_US : Region
    static member East_US : Region
    static member East_US_2 : Region
    static member North_Europe : Region
    static member West_Europe : Region
    static member Southeast_Asia : Region
    static member East_Asia : Region

/// The size of an Azure VM
type VMSize = VMSize of Size:string

[<Class>]
/// The VM sizes for Azure clusters 
type VMSizes = 
    static member A10 : VMSize
    static member A11 : VMSize
    static member A5 : VMSize
    static member A6 : VMSize
    static member A7 : VMSize
    static member A8 : VMSize
    static member A9 : VMSize
    /// Same as Extra Large
    static member A4 : VMSize
    /// Same as Extra Small
    static member A0 : VMSize
    /// Same as Large
    static member A3 : VMSize
    /// Same as Medium
    static member A2 : VMSize
    /// Same as Small
    static member A1 : VMSize
    static member Extra_Large : VMSize
    static member Large : VMSize
    static member Medium : VMSize
    static member Small : VMSize
    static member Extra_Small : VMSize
    static member Standard_D1 : VMSize
    static member Standard_D11 : VMSize
    static member Standard_D11_v2 : VMSize
    static member Standard_D12 : VMSize
    static member Standard_D12_v2 : VMSize
    static member Standard_D13 : VMSize
    static member Standard_D13_v2 : VMSize
    static member Standard_D14 : VMSize
    static member Standard_D14_v2 : VMSize
    static member Standard_D1_v2 : VMSize
    static member Standard_D2 : VMSize
    static member Standard_D2_v2 : VMSize
    static member Standard_D3 : VMSize
    static member Standard_D3_v2 : VMSize
    static member Standard_D4 : VMSize
    static member Standard_D4_v2 : VMSize
    static member Standard_D5_v2 : VMSize


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
    static member CreateCluster : pubSettingsFile : string * region : Region * ?ClusterName : string *  ?Subscription : string *  ?MBraceVersion : string * ?VMCount: int * ?StorageAccount: string * ?VMSize: VMSize * ?CloudServicePackage : string * ?ClusterLabel : string -> Configuration

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

