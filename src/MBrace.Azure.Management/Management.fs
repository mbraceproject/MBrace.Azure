namespace MBrace.Azure.Management

open System

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.PrettyPrinters
open MBrace.Azure
open MBrace.Azure.Runtime

type ConsoleLogger = MBrace.Runtime.ConsoleLogger

/// Represents an Azure service deployment handle object
[<Sealed; AutoSerializable(false)>]
type Deployment internal (client : SubscriptionClient, serviceName : string, logger : ISystemLogger) =
    let getDeploymentInfo = async {
        let! result = Compute.tryGetRunningDeployment client serviceName
        match result with
        | None -> return invalidOp <| sprintf "Deployment '%s' could not be found." serviceName
        | Some d ->  return d
    }

    let deployment = CacheAtom.Create(getDeploymentInfo, intervalMilliseconds = 1000)

    /// Deployment Cloud Service Name
    member __.ServiceName = serviceName
    /// MBrace.Azure configuration object for deployment.
    /// Used for initializing AzureCluster objects.
    member __.Configuration = deployment.Value.Configuration
    /// Gets the current instance information for the cloud service
    member __.Nodes = deployment.Value.Nodes |> List.toArray
    /// Time of current cloud service creation
    member __.CreatedTime = deployment.Value.CreatedTime
    /// Current deployment Status
    member __.DeploymentStatus = deployment.Value.DeploymentStatus
    /// Current service Status
    member __.ServiceStatus = deployment.Value.ServiceStatus

    /// <summary>
    ///     Prints deployment information to stdout  
    /// </summary>
    /// <param name="showInstances">Include information on individual worker instances. Defaults to true.</param>
    member __.ShowInfo([<O;D(null:obj)>]?showInstances : bool) =
        let showInstances = defaultArg showInstances true
        let current = deployment.Value
        let deploymentInfo = Compute.DeploymentReporter.Report([current], title = sprintf "Cloud Service %A" serviceName)
        if showInstances then
            let nodeInfo = Compute.InstanceReporter.Report(current.Nodes, title = "Cloud Service Instances")
            let nl = Environment.NewLine
            sprintf "%s%s%s" deploymentInfo nl nodeInfo |> Console.WriteLine
        else
            deploymentInfo |> Console.WriteLine

    /// Asynchronously deletes deployment from Azure
    member __.DeleteAsync() = Compute.deleteMBraceDeployment logger serviceName client
    /// Deletes deployment from Azure
    member __.Delete() = __.DeleteAsync() |> Async.RunSync


/// Client object for managing MBrace Cloud Service deployments for user-suppplied Azure subscriptions
[<Sealed; AutoSerializable(false)>]
type DeploymentManager private (subscriptions : SubscriptionsClient, defaultRegion : Region, _logger : ISystemLogger option, ?logLevel : LogLevel) =

    let logger = AttacheableLogger.Create(?logLevel = logLevel, makeAsynchronous = false)
    do _logger |> Option.iter(fun l -> ignore <| logger.AttachLogger l)

    let syncRoot = new obj()
    let mutable defaultRegion = defaultRegion
    let mutable subscriptions = subscriptions

    /// Attaches logger to the deployment manager instance
    member __.AttachLogger(l : ISystemLogger) = logger.AttachLogger l
    /// Gets or sets the default region used by the client instance
    member __.DefaultRegion
        with get () = defaultRegion
        and set reg = defaultRegion <- reg

    /// Lists all subscription names supplied in the current deployment manager instance
    member __.Subscriptions = subscriptions.Subscriptions |> Array.map (fun s -> s.Subscription.Name)
    /// Gets or sets the default subscription used by the deployment manager instance
    member __.DefaultSubscription 
        with get () = subscriptions.Default.Subscription.Name
        and set subId =
            lock syncRoot (fun () ->
                let sub = subscriptions.[subId]
                subscriptions <- { subscriptions with Default = sub })

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
    member __.DeployAsync(vmSize : VMSize, vmCount : int, [<O;D(null:obj)>]?serviceName : string, [<O;D(null:obj)>]?subscriptionId : string, [<O;D(null:obj)>]?region : Region, 
                            [<O;D(null:obj)>]?mbraceVersion : string, [<O;D(null:obj)>]?storageAccount : string, [<O;D(null:obj)>]?serviceBusAccount : string, [<O;D(null:obj)>]?cloudServicePackage : string, 
                            [<O;D(null:obj)>]?serviceLabel : string) : Async<Deployment> = async {

        if vmCount < 1 then invalidArg "vmCount" "must be positive value."
        let region = defaultArg region defaultRegion
        let client = subscriptions.GetClientByIdOrDefault(?id = subscriptionId)
        let serviceName = match serviceName with None -> Common.generateResourceName() | Some sn -> sn
        do! Compute.validateServiceName client serviceName

        logger.Logf LogLevel.Info "using vm size %A" vmSize
        let! packagePath, versionInfo = Compute.downloadServicePackage logger vmSize mbraceVersion cloudServicePackage
        logger.Logf LogLevel.Info "using cluster name %s" serviceName

        let! storageAccountName, storageConnectionString = Storage.resolveStorageAccount logger region storageAccount client
        logger.Logf LogLevel.Info "using storage account name %A" storageAccountName
        let! _, serviceBusNamespace, serviceBusConnectionString = ServiceBus.resolveNamespaceInfo logger region serviceBusAccount client
        logger.Logf LogLevel.Info "using service bus account %A" serviceBusNamespace

        let config = Compute.buildMBraceConfig serviceName vmCount storageConnectionString serviceBusConnectionString

        let clusterLabel = defaultArg serviceLabel (sprintf "MBrace cluster %A, package %s"  serviceName (defaultArg versionInfo "custom"))
        let! deployInfo = Compute.prepareMBraceServiceDeployment logger serviceName clusterLabel region packagePath config storageAccountName storageConnectionString serviceBusNamespace serviceBusConnectionString client
        do! Compute.beginDeploy false deployInfo client
        return new Deployment(client, serviceName, logger)
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
    member __.Deploy(vmSize : VMSize, vmCount : int, [<O;D(null:obj)>]?serviceName : string, [<O;D(null:obj)>]?subscriptionId : string, [<O;D(null:obj)>]?region : Region, 
                        [<O;D(null:obj)>]?mbraceVersion : string, [<O;D(null:obj)>]?storageAccount : string, [<O;D(null:obj)>]?serviceBusAccount : string, [<O;D(null:obj)>]?cloudServicePackage : string, [<O;D(null:obj)>]?serviceLabel : string) =
        __.DeployAsync(vmSize, vmCount, ?serviceName = serviceName, ?subscriptionId = subscriptionId, ?region = region, ?mbraceVersion = mbraceVersion,
                                ?storageAccount = storageAccount, ?serviceBusAccount = serviceBusAccount, ?cloudServicePackage = cloudServicePackage, ?serviceLabel = serviceLabel)
        |> Async.RunSync


    /// <summary>
    ///     Asynchronously fetches deployment of given service name
    /// </summary>
    /// <param name="serviceName">Deployment service name identifier.</param>
    /// <param name="subscriptionId">Subscription id to fetch deployments from. Defaults to manager instance subscription default.</param>
    member __.GetDeploymentAsync(serviceName : string, [<O;D(null:obj)>]?subscriptionId : string) = async {
        let client = subscriptions.GetClientByIdOrDefault(?id = subscriptionId)
        let! result = Compute.tryGetRunningDeployment client serviceName
        match result with
        | None -> return invalidOp <| sprintf "Deployment '%s' could not be found." serviceName
        | Some _ -> return new Deployment(client, serviceName, logger)
    }

    /// <summary>
    ///     Fetches deployment of given service name
    /// </summary>
    /// <param name="serviceName">Deployment service name identifier.</param>
    /// <param name="subscriptionId">Subscription id to fetch deployments from. Defaults to manager instance subscription default.</param>
    member __.GetDeployment(serviceName : string, [<O;D(null:obj)>]?subscriptionId : string) =
        __.GetDeploymentAsync(serviceName, ?subscriptionId = subscriptionId) |> Async.RunSync

    /// <summary>
    ///     Asynchronously fetches a list of all currently running deployments
    /// </summary>
    /// <param name="subscriptionId">Subscription id to fetch deployments from. Defaults to manager instance subscription default.</param>
    member __.GetDeploymentsAsync([<O;D(null:obj)>]?subscriptionId : string) = async {
        let client = subscriptions.GetClientByIdOrDefault(?id = subscriptionId)
        let! deployments = Compute.getRunningDeployments client
        return deployments |> Seq.map (fun d -> new Deployment(client, d.Name, logger)) |> Seq.toArray
    }

    /// <summary>
    ///     Fetches a list of all currently running deployments
    /// </summary>
    /// <param name="subscriptionId">Subscription id to fetch deployments from. Defaults to manager instance subscription default.</param>
    member __.GetDeployments([<O;D(null:obj)>]?subscriptionId : string) =
        __.GetDeploymentsAsync(?subscriptionId = subscriptionId) |> Async.RunSync

    /// <summary>
    ///     Prints a report on deployments to stdout.
    /// </summary>
    /// <param name="subscriptionId">Subscription id to fetch deployments from. Defaults to manager instance subscription default.</param>
    member __.ShowDeployments([<O;D(null:obj)>]?subscriptionId : string) =
        let client = subscriptions.GetClientByIdOrDefault(?id = subscriptionId)
        let deployments = Compute.getRunningDeployments client |> Async.RunSync
        let info = Compute.DeploymentReporter.Report(deployments, title = sprintf "Subscription: %A" client.Subscription.Name)
        Console.WriteLine(info)

    /// <summary>
    ///     Creates a new subscription manager instance using supplied set of Azure subscriptions
    /// </summary>
    /// <param name="subscriptions">Subscriptions to manage.</param>
    /// <param name="defaultRegion">Default Azure region for deployments.</param>
    /// <param name="defaultSubscriptionId">Default subscription id used by the manager instance. Defaults to first subscription in inputs.</param>
    /// <param name="logger">System logger used by the manager instance. Defaults to no logging.</param>
    /// <param name="logLevel">Log level used by the manager instance. Defaults to Info.</param>
    static member Create(subscriptions : seq<Subscription>, defaultRegion : Region, [<O;D(null:obj)>]?defaultSubscriptionId : string, [<O;D(null:obj)>]?logger : ISystemLogger, [<O;D(null:obj)>]?logLevel : LogLevel) =
        let client = SubscriptionsClient.Activate(subscriptions, ?defaultSubscriptionId = defaultSubscriptionId)
        new DeploymentManager(client, defaultRegion, logger, ?logLevel = logLevel)

    /// <summary>
    ///     Creates a new subscription manager instance using supplied set of Azure subscriptions
    /// </summary>
    /// <param name="publishSettings">Parsed PublishSettings record.</param>
    /// <param name="defaultRegion">Default Azure region for deployments.</param>
    /// <param name="defaultSubscriptionId">Default subscription id used by the manager instance. Defaults to first subscription in inputs.</param>
    /// <param name="logger">System logger used by the manager instance. Defaults to no logging.</param>
    /// <param name="logLevel">Log level used by the manager instance. Defaults to Info.</param>
    static member Create(publishSettings : PublishSettings, defaultRegion : Region, [<O;D(null:obj)>]?defaultSubscriptionId : string, [<O;D(null:obj)>]?logger : ISystemLogger, [<O;D(null:obj)>]?logLevel : LogLevel) =
        DeploymentManager.Create(publishSettings.Subscriptions, defaultRegion, ?logger = logger, ?defaultSubscriptionId = defaultSubscriptionId, ?logLevel = logLevel)

    /// <summary>
    ///     Creates a new subscription manager instance using supplied Azure subscription
    /// </summary>
    /// <param name="subscription">Subscription to manage.</param>
    /// <param name="defaultRegion">Default Azure region for deployments.</param>
    /// <param name="logger">System logger used by the manager instance. Defaults to no logging.</param>
    /// <param name="logLevel">Log level used by the manager instance. Defaults to Info.</param>
    static member Create(subscription : Subscription, defaultRegion : Region, [<O;D(null:obj)>]?logger : ISystemLogger, [<O;D(null:obj)>]?logLevel : LogLevel) =
        DeploymentManager.Create([subscription], defaultRegion, ?logger = logger, ?logLevel = logLevel)

    /// <summary>
    ///     Creates a new subscription manager instance using local Azure PublishSettings file.
    /// </summary>
    /// <param name="publishSettingsFile">Path to local PublishSettings file.</param>
    /// <param name="defaultRegion">Default Azure region for deployments.</param>
    /// <param name="defaultSubscriptionId">Default subscription id used by the manager instance. Defaults to first subscription in inputs.</param>
    /// <param name="logger">System logger used by the manager instance. Defaults to no logging.</param>
    /// <param name="logLevel">Log level used by the manager instance. Defaults to Info.</param>
    static member FromPublishSettingsFile(publishSettingsFile : string, defaultRegion : Region, [<O;D(null:obj)>]?defaultSubscriptionId : string, [<O;D(null:obj)>]?logger : ISystemLogger, [<O;D(null:obj)>]?logLevel : LogLevel) =
        let pubSettings = PublishSettings.ParseFile publishSettingsFile
        DeploymentManager.Create(pubSettings.Subscriptions, defaultRegion, ?defaultSubscriptionId = defaultSubscriptionId, ?logger = logger, ?logLevel = logLevel)


    //
    //  Static APIs
    //

    /// <summary>
    ///     Starts deployment of MBrace cloud service with supplied parameters.
    /// </summary>
    /// <param name="publishSettingsFile">Path to local PublishSettings file.</param>
    /// <param name="region">Azure region for deployments.</param>
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
    static member Deploy(pubSettingsFile : string, region : Region, vmSize : VMSize, vmCount : int, [<O;D(null:obj)>]?serviceName : string, [<O;D(null:obj)>]?subscriptionId : string, 
                            [<O;D(null:obj)>]?mbraceVersion : string, [<O;D(null:obj)>]?storageAccount : string, [<O;D(null:obj)>]?serviceBusAccount : string, [<O;D(null:obj)>]?cloudServicePackage : string, [<O;D(null:obj)>]?serviceLabel : string) =
        let manager = DeploymentManager.FromPublishSettingsFile(pubSettingsFile, defaultRegion = region, ?defaultSubscriptionId = subscriptionId, logger = new ConsoleLogger(true))
        manager.Deploy(vmSize, vmCount, ?serviceName = serviceName, ?subscriptionId = subscriptionId, ?mbraceVersion = mbraceVersion, 
                                ?storageAccount = storageAccount, ?serviceBusAccount = serviceBusAccount, ?cloudServicePackage = cloudServicePackage, ?serviceLabel = serviceLabel)


    /// <summary>
    ///     Deletes deployment of given name.
    /// </summary>
    /// <param name="publishSettingsFile">Path to local PublishSettings file.</param>
    /// <param name="serviceName">Service name to be deleted.</param>
    /// <param name="subscriptionId">Subscription id to fetch deployments from. Defaults to manager instance subscription default.</param>
    static member DeleteDeployment(pubSettingsFile : string, serviceName : string, [<O;D(null:obj)>]?subscriptionId : string) =
        let manager = DeploymentManager.FromPublishSettingsFile(pubSettingsFile, defaultRegion = Region.Define "", logger = new ConsoleLogger(true))
        let dpl = manager.GetDeployment(serviceName, ?subscriptionId = subscriptionId)
        dpl.Delete()

    /// <summary>
    ///     Gets a deployment handle to a running MBrace cloud service of given name.
    /// </summary>
    /// <param name="serviceName">Service name to be looked up.</param>
    /// <param name="subscriptionId">Subscription id to fetch deployments from. Defaults to manager instance subscription default.</param>
    static member GetDeployment(pubSettingsFile : string, serviceName : string, [<O;D(null:obj)>]?subscriptionId : string) =
        let manager = DeploymentManager.FromPublishSettingsFile(pubSettingsFile, defaultRegion = Region.Define "", logger = new ConsoleLogger(true))
        manager.GetDeployment(serviceName, ?subscriptionId = subscriptionId)


    /// <summary>
    ///     Prints a report on deployments to stdout.
    /// </summary>
    /// <param name="publishSettingsFile">Path to local PublishSettings file.</param>
    /// <param name="subscriptionId">Subscription id to fetch deployments from. Defaults to manager instance subscription default.</param>
    static member ShowDeployments(pubSettingsFile : string, [<O;D(null:obj)>]?subscriptionId : string) =
        let manager = DeploymentManager.FromPublishSettingsFile(pubSettingsFile, defaultRegion = Region.Define "", logger = new ConsoleLogger(true))
        manager.ShowDeployments(?subscriptionId = subscriptionId)


/// MBrace.Azure extension methods
[<AutoOpen>]
module Extensions =
    
    /// <summary>
    ///     Connects to supplied MBrace Azure deployment instance.
    ///     If successful returns a management handle object to the cluster.
    /// </summary>
    /// <param name="deployment">MBrace.Azure deployment instance.</param>
    /// <param name="clientId">MBrace.Azure client instance identifier.</param>
    /// <param name="faultPolicy">The default fault policy to be used by the cluster. Defaults to NoRetry.</param>
    /// <param name="logger">Custom logger to attach in client.</param>
    /// <param name="logLevel">Logger verbosity level.</param>
    type AzureCluster with
        static member Connect(deployment : Deployment, [<O;D(null:obj)>]?clientId : string, [<O;D(null:obj)>]?faultPolicy : FaultPolicy, [<O;D(null:obj)>]?logger : ISystemLogger, [<O;D(null:obj)>]?logLevel : LogLevel) = 
            AzureCluster.Connect(deployment.Configuration, ?clientId = clientId, ?faultPolicy = faultPolicy, ?logger = logger, ?logLevel = logLevel)