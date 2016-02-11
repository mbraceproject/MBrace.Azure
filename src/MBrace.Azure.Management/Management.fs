namespace MBrace.Azure.Management

open System
open System.Diagnostics

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.PrettyPrinters
open MBrace.Azure
open MBrace.Azure.Runtime
open System.IO

/// A system logger that writes entries to stdout
type ConsoleLogger = MBrace.Runtime.ConsoleLogger

/// Static configuration for MBrace.Azure.Management
type Config private () =
    /// Gets or sets the default logger used by the current processs
    static member val DefaultLogger = new NullLogger() :> ISystemLogger with get, set

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
    member __.Configuration = 
        let d = deployment.Value
        new Configuration(d.StorageAccount.ConnectionString, d.ServiceBusAccount.ConnectionString)

    /// Gets the deployment label
    member __.Label = deployment.Value.Label
    /// Gets the current instance information for the cloud service
    member __.Nodes = deployment.Value.VMInstances
    /// Numbers of VM instances currently in the deployment
    member __.InstanceCount = deployment.Value.VMInstances.Length
    /// Time of current cloud service creation
    member __.CreatedTime = deployment.Value.CreatedTime
    /// Gets the last modification time of the deployment
    member __.LastModified = deployment.Value.LastModified
    /// Gets the region of the deployment
    member __.Region = deployment.Value.Region
    /// Current deployment Status
    member __.DeploymentState = deployment.Value.DeploymentState
    /// Current service Status
    member __.ServiceStatus = deployment.Value.ServiceStatus
    /// Asynchronously fetches current deployment info record
    member __.GetInfoAsync() = deployment.GetValueAsync()
    /// Gets the storage account used by the mbrace deployment
    member __.StorageAccount = deployment.Value.StorageAccount
    /// Gets the service bus account used by the mbrace deployment
    member __.ServiceBusAccount = deployment.Value.ServiceBusAccount

    /// <summary>
    ///     Prints deployment information to stdout  
    /// </summary>
    /// <param name="showVmInstances">Include information on individual worker instances. Defaults to false.</param>
    member __.ShowInfo([<O;D(null:obj)>]?showVmInstances : bool) =
        let showVmInstances = defaultArg showVmInstances false
        let current = deployment.Value
        let deploymentInfo = Compute.DeploymentReporter.Report([current], title = sprintf "Cloud Service %A" serviceName)
        if showVmInstances && current.VMInstances.Length > 0 then
            let nodeInfo = Compute.InstanceReporter.Report(Array.toList current.VMInstances, title = "Cloud Service Instances")
            let nl = Environment.NewLine
            sprintf "%s%s%s" deploymentInfo nl nodeInfo |> Console.WriteLine
        else
            deploymentInfo |> Console.WriteLine

    /// Prints deployment vm instance information to stdout
    member __.ShowInstanceInfo() =
        let current = deployment.Value
        Compute.InstanceReporter.Report(Array.toList current.VMInstances, title = sprintf "Cloud Service %A" serviceName)
        |> Console.WriteLine

    /// <summary>
    ///      Asynchronously deletes deployment from Azure
    /// </summary>
    /// <param name="deleteStorageAccount">Delete the associated storage account. Defaults to false.</param>
    /// <param name="deleteServiceBusAccount">Delete the associated service bus account. Defaults to false.</param>
    member __.DeleteAsync([<O;D(null:obj)>]?deleteStorageAccount:bool, [<O;D(null:obj)>]?deleteServiceBusAccount:bool) = async {
        let deleteStorageAccount = defaultArg deleteStorageAccount false
        let deleteServiceBusAccount = defaultArg deleteServiceBusAccount false

        // if deleting, retrieve account info for deployment
        let! infoOpt = async {
            if deleteStorageAccount || deleteServiceBusAccount then
                let! info = deployment.GetValueAsync()
                return Some info
            else
                return None
        }

        do! Compute.deleteMBraceDeployment logger serviceName client

        // once the service has been deleted, delete accounts where applicable
        match infoOpt with
        | None -> ()
        | Some info ->
            if deleteStorageAccount then do! Storage.deleteStorageAccount logger info.StorageAccount.AccountName client
            if deleteServiceBusAccount then do! ServiceBus.deleteServiceBusAccount logger info.ServiceBusAccount.AccountName client
    }

    /// <summary>
    ///      Deletes deployment from Azure
    /// </summary>
    /// <param name="deleteStorageAccount">Delete the associated storage account. Defaults to false.</param>
    /// <param name="deleteServiceBusAccount">Delete the associated service bus account. Defaults to false.</param>
    member __.Delete([<O;D(null:obj)>]?deleteStorageAccount:bool, [<O;D(null:obj)>]?deleteServiceBusAccount:bool) =
        __.DeleteAsync(?deleteStorageAccount = deleteStorageAccount, ?deleteServiceBusAccount = deleteServiceBusAccount) |> Async.RunSync


    /// <summary>
    ///     Asynchronously resizes the given deployment instance count (scale-out).
    /// </summary>
    /// <param name="vmCount">New instance count.</param>
    member __.ResizeAsync(vmCount : int) = async {
        if vmCount < 0 then invalidArg "vmCount" "must be a non-negative value"
        do! Compute.resizeDeployment logger serviceName vmCount client
    }

    /// <summary>
    ///     Resizes the given deployment instance count (scale-out).
    /// </summary>
    /// <param name="vmCount">New instance count.</param>
    member __.Resize(vmCount : int) =
        __.ResizeAsync(vmCount) |> Async.RunSync

    /// <summary>
    ///     Asynchronously waits until provisioning of deployment has completed
    /// </summary>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds. Defaults to infinite timeout.</param>
    member __.AwaitProvisionAsync([<O;D(null:obj)>]?timeoutMilliseconds : int) : Async<unit> = async {
        let rec aux () = async {
            let! d = deployment.GetValueAsync()
            match d.DeploymentState with
            | DeploymentStatus.Provisioning _ ->
                do! Async.Sleep 2000
                return! aux()
            | _ -> return ()
        }

        return! Async.WithTimeout(aux(), ?timeoutMilliseconds = timeoutMilliseconds)
    }

    /// <summary>
    ///     Waits until provisioning of deployment has completed
    /// </summary>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds. Defaults to infinite timeout.</param>
    member __.AwaitProvision([<O;D(null:obj)>]?timeoutMilliseconds : int) : unit =
        __.AwaitProvisionAsync(?timeoutMilliseconds = timeoutMilliseconds) |> Async.RunSync

/// Storage Account manager API
[<Sealed; AutoSerializable(false)>]
type StorageManager internal (getParentInfo : unit -> ISystemLogger * SubscriptionClient * Region) =

    /// <summary>
    ///     Asynchronously fetches all storage account info for given region.
    /// </summary>
    /// <param name="region">Restrict account search to specific region. Defaults to all regions.</param>
    member __.GetAccountsAsync([<O;D(null:obj)>]?region : Region) = async {
        let logger,client,_ = getParentInfo()
        let! accountInfo = Storage.listAllStorageAccounts region client
        return! accountInfo |> Seq.map (fun aI -> Storage.resolveStorageAccount logger None aI.Name client) |> Async.Parallel
    }

    /// <summary>
    ///     Fetches all storage account info for given region.
    /// </summary>
    /// <param name="region">Restrict account search to specific region. Defaults to all regions.</param>
    member __.GetAccounts([<O;D(null:obj)>]?region : Region) : StorageAccount [] =
        __.GetAccountsAsync(?region = region) |> Async.RunSync

    /// <summary>
    ///     Asynchronously fetches existing azure storage account by name.
    /// </summary>
    /// <param name="accountName">Account name identifier.</param>
    member __.GetAccountAsync(accountName : string) : Async<StorageAccount> = async {
        let logger,client,_ = getParentInfo()
        return! Storage.resolveStorageAccount logger None accountName client
    }

    /// <summary>
    ///     Fetches azure existing storage account by name.
    /// </summary>
    /// <param name="accountName">Account name identifier.</param>
    member __.GetAccount(accountName : string) : StorageAccount =
        __.GetAccountAsync(accountName) |> Async.RunSync

    /// <summary>
    ///     Asynchronously creates a storage account with provided parameters.
    /// </summary>
    /// <param name="accountName">Storage account identifier.</param>
    /// <param name="region">Storage account default region. Defaults to deployment manager setting.</param>
    member __.CreateAccountAsync(accountName : string, [<O;D(null:obj)>]?region : Region) = async {
        let logger, client, defaultRegion = getParentInfo()
        let region = defaultArg region defaultRegion
        let! accountName = Storage.createMBraceStorageAccount logger region accountName client
        return! Storage.resolveStorageAccount logger None accountName client
    }

    /// <summary>
    ///     Creates a storage account with provided parameters.
    /// </summary>
    /// <param name="accountName">Storage account identifier.</param>
    /// <param name="region">Storage account default region. Defaults to deployment manager setting.</param>
    member __.CreateAccount(accountName : string, [<O;D(null:obj)>]?region : Region) : StorageAccount =
        __.CreateAccountAsync(accountName, ?region = region) |> Async.RunSync

    /// <summary>
    ///     Asynchronously deletes storage account by name.
    /// </summary>
    /// <param name="accountName">Account name identifier.</param>
    member __.DeleteAccountAsync(accountName : string) : Async<unit> = async {
        let logger, client, _ = getParentInfo()
        do! Storage.deleteStorageAccount logger accountName client
    }

    /// <summary>
    ///     Deletes storage account by name.
    /// </summary>
    /// <param name="accountName">Account name identifier.</param>
    member __.DeleteAccount(accountName : string) : unit =
        __.DeleteAccountAsync(accountName) |> Async.RunSync

    /// <summary>
    ///     Prints storage account info to stdout for given region.
    /// </summary>
    /// <param name="region">Restrict account search to specific region. Defaults to all regions.</param>
    member __.ShowAccounts([<O;D(null:obj)>]?region : Region) : unit =
        let _,client,_ = getParentInfo()
        let accountInfo = Storage.listAllStorageAccounts region client |> Async.RunSync
        let regionT = match region with Some r -> sprintf " [%s]" r.Id | None -> ""
        let title = sprintf "Azure Storage Accounts for subscription %A%s" client.Subscription.Name regionT
        Storage.StorageAccountReporter.Report(Array.toList accountInfo, title = title) |> Console.WriteLine


/// Service Bus Account manager API
[<Sealed; AutoSerializable(false)>]
type ServiceBusManager internal (getParentInfo : unit -> ISystemLogger * SubscriptionClient * Region) =

    /// <summary>
    ///     Asynchronously fetches all service bus account info for given region.
    /// </summary>
    /// <param name="region">Restrict account search to specific region. Defaults to all regions.</param>
    member __.GetAccountsAsync([<O;D(null:obj)>]?region : Region) = async {
        let logger,client,_ = getParentInfo()
        let! accountInfo = ServiceBus.listAllServiceBusAccounts region client
        return! accountInfo |> Seq.map (fun aI -> ServiceBus.resolveServiceBusAccount logger None aI.Name client) |> Async.Parallel
    }

    /// <summary>
    ///     Fetches all service bus account info for given region.
    /// </summary>
    /// <param name="region">Restrict account search to specific region. Defaults to all regions.</param>
    member __.GetAccounts([<O;D(null:obj)>]?region : Region) : ServiceBusAccount [] =
        __.GetAccountsAsync(?region = region) |> Async.RunSync

    /// <summary>
    ///     Asynchronously fetches existing azure service bus account by name.
    /// </summary>
    /// <param name="accountName">Account name identifier.</param>
    member __.GetAccountAsync(accountName : string) : Async<ServiceBusAccount> = async {
        let logger,client,_ = getParentInfo()
        return! ServiceBus.resolveServiceBusAccount logger None accountName client
    }

    /// <summary>
    ///     Fetches azure existing service bus account by name.
    /// </summary>
    /// <param name="accountName">Account name identifier.</param>
    member __.GetAccount(accountName : string) : ServiceBusAccount =
        __.GetAccountAsync(accountName) |> Async.RunSync

    /// <summary>
    ///     Asynchronously creates a service bus account with provided parameters.
    /// </summary>
    /// <param name="accountName">service bus account identifier.</param>
    /// <param name="region">service bus account default region. Defaults to deployment manager setting.</param>
    member __.CreateAccountAsync(accountName : string, [<O;D(null:obj)>]?region : Region) = async {
        let logger, client, defaultRegion = getParentInfo()
        let region = defaultArg region defaultRegion
        let! accountName = ServiceBus.createServiceBusAccount logger region accountName client
        return! ServiceBus.resolveServiceBusAccount logger None accountName client
    }

    /// <summary>
    ///     Creates a service bus account with provided parameters.
    /// </summary>
    /// <param name="accountName">service bus account identifier.</param>
    /// <param name="region">service bus account default region. Defaults to deployment manager setting.</param>
    member __.CreateAccount(accountName : string, [<O;D(null:obj)>]?region : Region) : ServiceBusAccount =
        __.CreateAccountAsync(accountName, ?region = region) |> Async.RunSync

    /// <summary>
    ///     Asynchronously deletes service bus account by name.
    /// </summary>
    /// <param name="accountName">Account name identifier.</param>
    member __.DeleteAccountAsync(accountName : string) : Async<unit> = async {
        let logger, client,_ = getParentInfo()
        do! ServiceBus.deleteServiceBusAccount logger accountName client
    }

    /// <summary>
    ///     Deletes service bus account by name.
    /// </summary>
    /// <param name="accountName">Account name identifier.</param>
    member __.DeleteAccount(accountName : string) : unit =
        __.DeleteAccountAsync(accountName) |> Async.RunSync

    /// <summary>
    ///     Prints service bus account info to stdout for given region.
    /// </summary>
    /// <param name="region">Restrict account search to specific region. Defaults to all regions.</param>
    member __.ShowAccounts([<O;D(null:obj)>]?region : Region) : unit =
        let _,client,_ = getParentInfo()
        let accountInfo = ServiceBus.listAllServiceBusAccounts region client |> Async.RunSync
        let regionT = match region with Some r -> sprintf " [%s]" r.Id | None -> ""
        let title = sprintf "Azure Service Bus Accounts for subscription %A%s" client.Subscription.Name regionT
        ServiceBus.ServiceBusAccountReporter.Report(Array.toList accountInfo, title = title) |> Console.WriteLine


/// Client object for managing MBrace Cloud Service deployments for user-suppplied Azure subscription
[<Sealed; AutoSerializable(false)>]
type SubscriptionManager private (client : SubscriptionClient, defaultRegion : Region, _logger : ISystemLogger, logLevel : LogLevel) =

    let logger = AttacheableLogger.Create(logLevel, makeAsynchronous = false)
    let _ = logger.AttachLogger _logger

    let mutable defaultRegion = defaultRegion

    let getState () = logger :> ISystemLogger, client, defaultRegion

    let storageManager = new StorageManager(getState)
    let serviceBusManager = new ServiceBusManager(getState)

    /// Attaches logger to the deployment manager instance
    member __.AttachLogger(l : ISystemLogger) = logger.AttachLogger l
    /// Gets or sets the default region used by the client instance
    member __.DefaultRegion
        with get () = defaultRegion
        and set reg = defaultRegion <- reg

    /// Subscription name
    member __.SubscriptionName = client.Subscription.Name
    /// Subscription identifier
    member __.SubscriptionId = client.Subscription.Id

    /// Storage account management client
    [<DebuggerBrowsable(DebuggerBrowsableState.Never)>]
    member __.Storage = storageManager
    /// Service Bus account management client
    [<DebuggerBrowsable(DebuggerBrowsableState.Never)>]
    member __.ServiceBus = serviceBusManager

    //
    // #region Deployment methods
    //

    /// <summary>
    ///     Asynchronously starts deployment of MBrace cloud service with supplied parameters.
    /// </summary>
    /// <param name="vmCount">VM instance count.</param>
    /// <param name="serviceName">Service name identifier. Defaults to auto-generated name.</param>
    /// <param name="region">Region for service deployment. Defaults to manager instance default region.</param>
    /// <param name="vmSize">VM size used for deployment. Defaults to Medium instances.</param>
    /// <param name="mbraceVersion">MBrace version string used for .cspkg resolution. Defaults to current version.</param>
    /// <param name="storageAccount">Storage account name or connection string used by MBrace service. Defaults to self-allocated storage account.</param>
    /// <param name="serviceBusAccount">Service bus account name or connection string used by MBrace service. Defaults to self-allocation service bus account.</param>
    /// <param name="cloudServicePackage">Path or Uri to MBrace cloud service package to be deployed to Service. Defaults to .cspkg resolved from github.</param>
    /// <param name="clusterLabel">User-supplied service label. Defaults to library generated label.</param>
    /// <param name="enableDiagnostics">Enable Azure diagnostics for deployment using storage account. Defaults to false.</param>
    /// <param name="reuseAccounts">Reuse existing inactive mbrace storage/service bus accounts. Defaults to true.</param>
    member __.ProvisionAsync(vmCount : int, [<O;D(null:obj)>]?serviceName : string, [<O;D(null:obj)>]?region : Region, [<O;D(null:obj)>]?vmSize : VMSize,  
                                [<O;D(null:obj)>]?mbraceVersion : string, [<O;D(null:obj)>]?storageAccount : string, [<O;D(null:obj)>]?serviceBusAccount : string, [<O;D(null:obj)>]?cloudServicePackage : string, 
                                [<O;D(null:obj)>]?clusterLabel : string, [<O;D(null:obj)>]?enableDiagnostics : bool, [<O;D(null:obj)>]?reuseAccounts : bool) : Async<Deployment> = async {

        if vmCount < 1 then invalidArg "vmCount" "must be positive value."
        let enableDiagnostics = defaultArg enableDiagnostics false
        let reuseAccounts = defaultArg reuseAccounts true
        let region = defaultArg region defaultRegion
        let vmSize = defaultArg vmSize VMSize.Medium

        let serviceName = match serviceName with None -> Common.generateResourceName() | Some sn -> sn
        do! Compute.validateServiceName client serviceName

        if Option.isNone cloudServicePackage then
            logger.Logf LogLevel.Info "using vm size %A" vmSize
            do! Infrastructure.checkCompatibility region vmSize client
        else
            logger.Logf LogLevel.Info "using custom cspkg"

        logger.Logf LogLevel.Info "using cluster name %s" serviceName

        let! storageAccountT = Storage.getDeploymentStorageAccount logger reuseAccounts region storageAccount client |> Async.StartChild
        let! serviceBusAccount = ServiceBus.getDeploymentServiceBusAccount logger reuseAccounts region serviceBusAccount client
        let! storageAccount = storageAccountT
        let packageDetails, customClusterLabel =
            match cloudServicePackage with
            | None ->
                let mbraceVersion = (defaultArg mbraceVersion Common.defaultMBraceVersion)
                Compute.Official(mbraceVersion, vmSize), (sprintf "mbrace-%s" mbraceVersion)
            | Some uri ->
                let uri = Uri uri
                (if uri.IsFile then Compute.CustomLocal(FileInfo uri.LocalPath)
                 else Compute.CustomRemote uri), "custom-cspkg"

        let clusterLabel = defaultArg clusterLabel customClusterLabel        

        do! Compute.createDeployment logger serviceName clusterLabel region packageDetails false enableDiagnostics vmCount storageAccount serviceBusAccount client
        return new Deployment(client, serviceName, logger)
    }

    /// <summary>
    ///     Starts deployment of MBrace cloud service with supplied parameters.
    /// </summary>
    /// <param name="vmCount">VM instance count.</param>
    /// <param name="serviceName">Service name identifier. Defaults to auto-generated name.</param>
    /// <param name="region">Region for service deployment. Defaults to manager instance default region.</param>
    /// <param name="vmSize">VM size used for deployment. Defaults to A2 instances.</param>
    /// <param name="mbraceVersion">MBrace version string used for .cspkg resolution. Defaults to current version.</param>
    /// <param name="storageAccount">Storage account name or connection string used by MBrace service. Defaults to self-allocated storage account.</param>
    /// <param name="serviceBusAccount">Service bus account name or connection string used by MBrace service. Defaults to self-allocation service bus account.</param>
    /// <param name="cloudServicePackage">Path or Uri to MBrace cloud service package to be deployed to Service. Defaults to .cspkg resolved from github.</param>
    /// <param name="clusterLabel">User-supplied service label. Defaults to library generated label.</param>
    /// <param name="enableDiagnostics">Enable Azure diagnostics for deployment using storage account. Defaults to false.</param>
    /// <param name="reuseAccounts">Reuse existing inactive mbrace storage/service bus accounts. Defaults to true.</param>
    member __.Provision(vmCount : int, [<O;D(null:obj)>]?serviceName : string, [<O;D(null:obj)>]?region : Region, [<O;D(null:obj)>]?vmSize : VMSize, 
                        [<O;D(null:obj)>]?mbraceVersion : string, [<O;D(null:obj)>]?storageAccount : string, [<O;D(null:obj)>]?serviceBusAccount : string, [<O;D(null:obj)>]?cloudServicePackage : string, 
                        [<O;D(null:obj)>]?clusterLabel : string, [<O;D(null:obj)>]?enableDiagnostics : bool, [<O;D(null:obj)>]?reuseAccounts : bool) =
        __.ProvisionAsync(vmCount, ?serviceName = serviceName, ?region = region, ?mbraceVersion = mbraceVersion, ?vmSize = vmSize,
                                ?storageAccount = storageAccount, ?serviceBusAccount = serviceBusAccount, ?cloudServicePackage = cloudServicePackage, 
                                ?clusterLabel = clusterLabel, ?enableDiagnostics = enableDiagnostics, ?reuseAccounts = reuseAccounts)
        |> Async.RunSync


    /// <summary>
    ///     Asynchronously fetches deployment of given service name
    /// </summary>
    /// <param name="serviceName">Deployment service name identifier.</param>
    member __.GetDeploymentAsync(serviceName : string) = async {
        let! result = Compute.tryGetRunningDeployment client serviceName
        match result with
        | None -> return invalidOp <| sprintf "Deployment %A could not be found." serviceName
        | Some _ -> return new Deployment(client, serviceName, logger)
    }

    /// <summary>
    ///     Fetches deployment of given service name
    /// </summary>
    /// <param name="serviceName">Deployment service name identifier.</param>
    member __.GetDeployment(serviceName : string) =
        __.GetDeploymentAsync(serviceName) |> Async.RunSync

    /// Asynchronously fetches a list of all currently running deployments
    member __.GetDeploymentsAsync() = async {
        let! deployments = Compute.getRunningDeployments client
        return deployments |> Seq.map (fun d -> new Deployment(client, d.Name, logger)) |> Seq.toArray
    }

    /// Fetches a list of all currently running deployments
    member __.GetDeployments() = __.GetDeploymentsAsync() |> Async.RunSync

    /// <summary>
    ///     Asynchronously deletes deployment of given name.
    /// </summary>
    /// <param name="serviceName">Cloud service name of deployment.</param>
    /// <param name="deleteStorageAccount">Delete the associated storage account. Defaults to false.</param>
    /// <param name="deleteServiceBusAccount">Delete the associated service bus account. Defaults to false.</param>
    member __.DeleteDeploymentAsync(serviceName : string, [<O;D(null:obj)>]?deleteStorageAccount:bool, [<O;D(null:obj)>]?deleteServiceBusAccount:bool) = async {
        let! deployment = __.GetDeploymentAsync(serviceName)
        return! deployment.DeleteAsync(?deleteStorageAccount = deleteStorageAccount, ?deleteServiceBusAccount = deleteServiceBusAccount)
    }

    /// <summary>
    ///     Deletes deployment of given name.
    /// </summary>
    /// <param name="serviceName">Cloud service name of deployment.</param>
    /// <param name="deleteStorageAccount">Delete the associated storage account. Defaults to false.</param>
    /// <param name="deleteServiceBusAccount">Delete the associated service bus account. Defaults to false.</param>
    member __.DeleteDeployment(serviceName : string, [<O;D(null:obj)>]?deleteStorageAccount:bool, [<O;D(null:obj)>]?deleteServiceBusAccount:bool) =
        __.DeleteDeploymentAsync(serviceName, ?deleteStorageAccount = deleteStorageAccount, ?deleteServiceBusAccount = deleteServiceBusAccount) |> Async.RunSync

    /// <summary>
    ///     Asynchronously resizes deployment of given name to supplied instance count (scale out).
    /// </summary>
    /// <param name="serviceName">Service name identifier.</param>
    /// <param name="vmCount">New VM instance count.</param>
    member __.ResizeDeploymentAsync(serviceName : string, vmCount : int) = async {
        let! deployment = __.GetDeploymentAsync(serviceName)
        return! deployment.ResizeAsync(vmCount)
    }

    /// <summary>
    ///     Asynchronously resizes deployment of given name to supplied instance count (scale out).
    /// </summary>
    /// <param name="serviceName">Service name identifier.</param>
    /// <param name="vmCount">New VM instance count.</param>
    member __.ResizeDeployment(serviceName : string, vmCount : int) =
        __.ResizeDeploymentAsync(serviceName, vmCount) |> Async.RunSync

    /// <summary>
    ///     Prints a report on deployments to stdout.
    /// </summary>
    member __.ShowDeployments() =
        let deployments = Compute.getRunningDeployments client |> Async.RunSync
        let info = Compute.DeploymentReporter.Report(deployments, title = sprintf "Subscription: %A" client.Subscription.Name)
        Console.WriteLine(info)

    //
    // #region Factory methods
    //

    /// <summary>
    ///     Creates a new subscription manager instance using supplied Azure subscription
    /// </summary>
    /// <param name="subscription">Subscription to manage.</param>
    /// <param name="defaultRegion">Default Azure region for deployments.</param>
    /// <param name="logger">System logger used by the manager instance. Defaults to no logging.</param>
    /// <param name="logLevel">Log level used by the manager instance. Defaults to Info.</param>
    static member Create(subscription : Subscription, defaultRegion : Region, [<O;D(null:obj)>]?logger : ISystemLogger, [<O;D(null:obj)>]?logLevel : LogLevel) =
        let logger = defaultArg logger Config.DefaultLogger
        let logLevel = defaultArg logLevel LogLevel.Info
        let client = SubscriptionClient.Activate(subscription)
        new SubscriptionManager(client, defaultRegion, logger, logLevel = logLevel)

    /// <summary>
    ///     Creates a new subscription manager instance using supplied set of Azure subscriptions
    /// </summary>
    /// <param name="publishSettings">Parsed PublishSettings record.</param>
    /// <param name="defaultRegion">Default Azure region for deployments.</param>
    /// <param name="subscriptionId">Subscription identifier to be used by the manager instance. Must be specified if pubsettings defines more than one subscription.</param>
    /// <param name="logger">System logger used by the manager instance. Defaults to no logging.</param>
    /// <param name="logLevel">Log level used by the manager instance. Defaults to Info.</param>
    static member FromPublishSettings(publishSettings : PublishSettings, defaultRegion : Region, [<O;D(null:obj)>]?subscriptionId : string, [<O;D(null:obj)>]?logger : ISystemLogger, [<O;D(null:obj)>]?logLevel : LogLevel) =
        let subscription =
            match subscriptionId with
            | Some id -> publishSettings.GetSubscriptionById id
            | None -> 
                match publishSettings.Subscriptions with
                | [||] -> invalidArg "publishSettingsFile" "PublishSettings file must define at least one Azure subscription."
                | [|s|] -> s
                | subs -> 
                    let subInfo = subs |> Seq.map (fun s -> sprintf "\t* %A" s.Name) |> String.concat Environment.NewLine
                    let msg = sprintf "PublishSettings declares multiple subscriptions, please specify either of the following:%s%s" Environment.NewLine subInfo
                    invalidArg "subscriptionId" msg

        SubscriptionManager.Create(subscription, defaultRegion, ?logger = logger, ?logLevel = logLevel)


    /// <summary>
    ///     Creates a new subscription manager instance using local Azure PublishSettings file.
    /// </summary>
    /// <param name="publishSettingsFile">Path to local PublishSettings file.</param>
    /// <param name="defaultRegion">Default Azure region for deployments.</param>
    /// <param name="subscriptionId">Subscription identifier to be used by the manager instance. Must be specified if pubsettings defines more than one subscription.</param>
    /// <param name="logger">System logger used by the manager instance. Defaults to no logging.</param>
    /// <param name="logLevel">Log level used by the manager instance. Defaults to Info.</param>
    static member FromPublishSettingsFile(publishSettingsFile : string, defaultRegion : Region, [<O;D(null:obj)>]?subscriptionId : string, [<O;D(null:obj)>]?logger : ISystemLogger, [<O;D(null:obj)>]?logLevel : LogLevel) =
        let pubSettings = PublishSettings.ParseFile publishSettingsFile
        SubscriptionManager.FromPublishSettings(pubSettings, defaultRegion, ?subscriptionId = subscriptionId, ?logger = logger, ?logLevel = logLevel)