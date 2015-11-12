namespace MBrace.Azure.Management

open System

open Microsoft.WindowsAzure.Management.Compute.Models

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils.PrettyPrinters
open MBrace.Azure
open MBrace.Azure.Runtime

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

        if vmCount < 1 then invalidArg "vmCount" "must be positive value."
        let region = defaultArg region defaultRegion
        let client = pubSettings.GetClientByIdOrDefault(?id = subscriptionId)
        let serviceName = match serviceName with None -> generateResourceName() | Some sn -> sn
        do! Deployment.validateServiceName client serviceName

        logger.Logf LogLevel.Info "using vm size %A" vmSize
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
    static member BeginDeploy(pubSettingsFile : string, region : Region, vmSize : VMSize, vmCount : int, ?serviceName : string, ?subscriptionId : string, 
                                ?mbraceVersion : string, ?storageAccount : string, ?serviceBusAccount : string, ?cloudServicePackage : string, ?serviceLabel : string) =
        let manager = DeploymentManager.FromPublishSettingsFile(pubSettingsFile, defaultRegion = region, ?defaultSubscriptionId = subscriptionId, logger = new ConsoleLogger(true))
        manager.BeginDeploy(vmSize, vmCount, ?serviceName = serviceName, ?subscriptionId = subscriptionId, ?mbraceVersion = mbraceVersion, 
                                ?storageAccount = storageAccount, ?serviceBusAccount = serviceBusAccount, ?cloudServicePackage = cloudServicePackage, ?serviceLabel = serviceLabel)


    /// <summary>
    ///     Deletes deployment of given name.
    /// </summary>
    /// <param name="publishSettingsFile">Path to local PublishSettings file.</param>
    /// <param name="serviceName">Service name to be deleted.</param>
    /// <param name="subscriptionId">Subscription id to fetch deployments from. Defaults to manager instance subscription default.</param>
    static member DeleteDeployment(pubSettingsFile : string, serviceName : string, ?subscriptionId : string) =
        let manager = DeploymentManager.FromPublishSettingsFile(pubSettingsFile, defaultRegion = Region.Define "", logger = new ConsoleLogger(true))
        manager.DeleteDeployment(serviceName, ?subscriptionId = subscriptionId)

    /// <summary>
    ///     Gets the Azure cluster configuration assigned to the given
    ///     MBrace service deployment.
    /// </summary>
    /// <param name="serviceName">Service name to be looked up.</param>
    /// <param name="subscriptionId">Subscription id to fetch deployments from. Defaults to manager instance subscription default.</param>
    static member GetConfiguration(pubSettingsFile : string, serviceName : string, ?subscriptionId : string) =
        let manager = DeploymentManager.FromPublishSettingsFile(pubSettingsFile, defaultRegion = Region.Define "", logger = new ConsoleLogger(true))
        manager.GetConfiguration(serviceName, ?subscriptionId = subscriptionId)


    /// <summary>
    ///     Prints a report on deployments to stdout.
    /// </summary>
    /// <param name="publishSettingsFile">Path to local PublishSettings file.</param>
    /// <param name="subscriptionId">Subscription id to fetch deployments from. Defaults to manager instance subscription default.</param>
    static member ShowDeployments(pubSettingsFile : string, ?subscriptionId : string) =
        let manager = DeploymentManager.FromPublishSettingsFile(pubSettingsFile, defaultRegion = Region.Define "", logger = new ConsoleLogger(true))
        manager.ShowDeployments(?subscriptionId = subscriptionId)