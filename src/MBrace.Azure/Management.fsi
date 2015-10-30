
namespace MBrace.Azure


[<Class>]
/// The regions in which Azure clusters can be created
type Regions = 
    static member South_Central_US : string
    static member West_US  : string
    static member Central_US  : string
    static member East_US  : string
    static member East_US_2  : string
    static member North_Europe  : string
    static member West_Europe  : string
    static member Southeast_Asia  : string
    static member East_Asia  : string

[<Class>]
/// The VM sizes for Azure clusters 
type VMSizes = 
     static member A10  : string
     static member A11  : string
     static member A5  : string
     static member A6  : string
     static member A7  : string
     static member A8  : string
     static member A9  : string
     static member A4  : string
     static member A0  : string
     static member A3  : string
     static member A2  : string
     static member A1  : string
     static member ExtraLarge  : string
     static member Large  : string
     static member Medium  : string
     static member Small  : string
     static member ExtraSmall  : string
     static member Standard_D1  : string
     static member Standard_D11  : string
     static member Standard_D11_v2  : string
     static member Standard_D12  : string
     static member Standard_D12_v2  : string
     static member Standard_D13  : string
     static member Standard_D13_v2  : string
     static member Standard_D14  : string
     static member Standard_D14_v2  : string
     static member Standard_D1_v2  : string
     static member Standard_D2  : string
     static member Standard_D2_v2  : string
     static member Standard_D3  : string
     static member Standard_D3_v2  : string
     static member Standard_D4  : string
     static member Standard_D4_v2  : string
     static member Standard_D5_v2  : string

[<Class>]
type Management = 

    /// <summary>Provision an MBrace cluster in the subscription from the pubsettings file</summary>
    /// <param name="pubSettingsFile">The path to the pubsettings file. Download from https://manage.windowsazure.com/publishsettings</param>
    /// <param name="region">The Azure region in which to create the cluster. Choose from Regions.*</param>
    /// <param name="ClusterName">The name of the cluster</param>
    /// <param name="Subscription">The subscription to use</param>
    /// <param name="MBraceVersion">The MBrace software version id to use. Ignored if using an explicit package.</param>
    /// <param name="VMCount">The number of virtual machines to allocate in the cluster.</param>
    /// <param name="VMSize">The size of virtual machines to allocate in the cluster. Use one of VMSizes.*</param>
    /// <param name="StorageAccount">The name of the storage ccount to use. Defaults to reusing a suitable existing account if available, otherwise creates a new one.</param>
    /// <param name="CloudServicePackage">An explicit cloud service package to use.</param>
    /// <param name="ClusterLabel">The label to give the deployment of the cloud service.</param>
    static member CreateCluster : pubSettingsFile : string * region : string * ?ClusterName : string *  ?Subscription : string *  ?MBraceVersion : string * ?VMCount: int * ?StorageAccount: string * ?VMSize: string * ?CloudServicePackage : string * ?ClusterLabel : string -> string

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

