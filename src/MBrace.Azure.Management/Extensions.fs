namespace MBrace.Azure.Management

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Azure

/// MBrace.Azure.Management extension methods
[<AutoOpen>]
module Extensions =
    
    type AzureCluster with
        /// <summary>
        ///     Connects to supplied MBrace Azure deployment instance.
        ///     If successful returns a management handle object to the cluster.
        /// </summary>
        /// <param name="deployment">MBrace.Azure deployment instance.</param>
        /// <param name="clientId">MBrace.Azure client instance identifier.</param>
        /// <param name="faultPolicy">The default fault policy to be used by the cluster. Defaults to NoRetry.</param>
        /// <param name="logger">Custom logger to attach in client.</param>
        /// <param name="logLevel">Logger verbosity level.</param>
        static member Connect(deployment : Deployment, [<O;D(null:obj)>]?clientId : string, [<O;D(null:obj)>]?faultPolicy : FaultPolicy, [<O;D(null:obj)>]?logger : ISystemLogger, [<O;D(null:obj)>]?logLevel : LogLevel) = 
            AzureCluster.Connect(deployment.Configuration, ?clientId = clientId, ?faultPolicy = faultPolicy, ?logger = logger, ?logLevel = logLevel)

    type AzureBlobStorage with
        /// <summary>
        ///      Creates a blob storage client object from given storage account object.
        /// </summary>
        /// <param name="account">Azure blob storage account.</param>
        /// <param name="serializer">Serializer for use with store. Defaults to FsPickler binary serializer.</param>
        static member FromStorageAccount(account : StorageAccount, [<O;D(null:obj)>]?serializer : ISerializer) =
            AzureBlobStorage.FromConnectionString(account.ConnectionString, ?serializer = serializer)


    type Deployment with
        /// <summary>
        ///     Starts deployment of MBrace cloud service with supplied parameters.
        /// </summary>
        /// <param name="publishSettingsFile">Path to your downloaded .publishsettings file.</param>
        /// <param name="region">Region for service deployment.</param>
        /// <param name="vmCount">VM instance count.</param>
        /// <param name="vmSize">VM size used for deployment. Defaults to A2 instances.</param>
        /// <param name="cloudServicePackage">Path or Uri to MBrace cloud service package to be deployed to Service. Defaults to .cspkg resolved from github.</param>
        /// <param name="subscriptionId">Subscription identifier to be used by the manager instance.</param>
        /// <param name="logger">System logger used by the manager instance. Defaults to console logger.</param>
        /// <param name="logLevel">Log level used by the manager instance. Defaults to Info.</param>
        /// <param name="mbraceVersion">MBrace version string used for .cspkg resolution. Defaults to current version.</param>
        /// <param name="storageAccount">Storage account name or connection string used by MBrace service. Defaults to self-allocated storage account.</param>
        /// <param name="serviceBusAccount">Service bus account name or connection string used by MBrace service. Defaults to self-allocation service bus account.</param>
        /// <param name="serviceName">Service name identifier. Defaults to auto-generated name.</param>
        /// <param name="serviceLabel">User-supplied service label. Defaults to library generated label.</param>
        /// <param name="enableDiagnostics">Enable Azure diagnostics for deployment using storage account. Defaults to false.</param>
        static member Provision(publishSettingsFile : string, region : Region, vmCount : int, [<O;D(null:obj)>]?vmSize : VMSize, [<O;D(null:obj)>]?cloudServicePackage : string,
                                [<O;D(null:obj)>]?subscriptionId : string, [<O;D(null:obj)>]?logger : ISystemLogger, [<O;D(null:obj)>]?logLevel : LogLevel,
                                [<O;D(null:obj)>]?mbraceVersion : string, [<O;D(null:obj)>]?storageAccount : string, [<O;D(null:obj)>]?serviceBusAccount : string,
                                [<O;D(null:obj)>]?serviceName : string, [<O;D(null:obj)>]?serviceLabel : string, [<O;D(null:obj)>]?enableDiagnostics : bool) : Deployment =

            let logger = match logger with Some l -> l | None -> new ConsoleLogger() :> _
            let manager = SubscriptionManager.FromPublishSettingsFile(publishSettingsFile, region, ?subscriptionId = subscriptionId, logger = logger, ?logLevel = logLevel)
            manager.Provision(vmCount, ?serviceName = serviceName, ?vmSize = vmSize, ?storageAccount = storageAccount, ?serviceBusAccount = serviceBusAccount, 
                            ?cloudServicePackage = cloudServicePackage, ?serviceLabel = serviceLabel, ?enableDiagnostics = enableDiagnostics, ?mbraceVersion = mbraceVersion)

        /// <summary>
        ///     Gets an already existing deployment instance using provided publish settings and service name.
        /// </summary>
        /// <param name="publishSettingsFile">Path to your downloaded .publishsettings file.</param>
        /// <param name="serviceName">Service name identifier.</param>
        /// <param name="subscriptionId">Subscription identifier to be used by the manager instance.</param>
        static member GetDeployment(publishSettingsFile : string, serviceName : string, [<O;D(null:obj)>]?subscriptionId : string) : Deployment =
            let manager = SubscriptionManager.FromPublishSettingsFile(publishSettingsFile, Region.Define "", ?subscriptionId = subscriptionId)
            manager.GetDeployment serviceName