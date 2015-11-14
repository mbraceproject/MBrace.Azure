namespace MBrace.Azure.Management

open System

open Microsoft.WindowsAzure.Management.ServiceBus
open Microsoft.WindowsAzure.Management.ServiceBus.Models

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils.Retry
open MBrace.Azure.Runtime

module internal ServiceBus =

    let listAllServiceBusAccounts (client:SubscriptionClient) = async {
        let! (listed : _) = client.ServiceBus.Namespaces.ListAsync()
        return listed |> Seq.toArray
    }

    let listLocalMBraceServiceBusAccounts (region : Region) (client : SubscriptionClient) = async {
        let! accounts = listAllServiceBusAccounts client
        return
            accounts
            |> Seq.filter (fun account -> account.Region = region.Id)
            |> Seq.filter(fun account -> account.Name.StartsWith Common.resourcePrefix)
            |> Seq.map(fun account -> account.Name)
            |> Seq.toArray
    }

    let rec private waitUntilState checkState ns (client:SubscriptionClient) = async {
        let! result = client.ServiceBus.Namespaces.GetAsync ns |> Async.AwaitTaskCorrect |> Async.Catch
        let status = match result with Choice1Of2 ns -> ns.Namespace.Status | Choice2Of2 _ -> ""
        if not (checkState status) then
            do! Async.Sleep 2000
            return! waitUntilState checkState ns client
    }

    /// Creates a new service bus account in supplied region
    let createServiceBusAccount (logger : ISystemLogger) (region : Region) (namespaceName : string) (client:SubscriptionClient) = async {
        let aux () = async {
            let! (availability : CheckNamespaceAvailabilityResponse) = client.ServiceBus.Namespaces.CheckAvailabilityAsync namespaceName
            if not availability.IsAvailable then
                return invalidOp <| sprintf "ServiceBus namespace name %A is not available" namespaceName

            let! (result : ServiceBusNamespaceResponse) = client.ServiceBus.Namespaces.CreateAsync(namespaceName, region.Id)
            if result.StatusCode <> Net.HttpStatusCode.OK then 
                return invalidOp <| sprintf "Failed to create service bus: %O" result.StatusCode
            else 
                do! client |> waitUntilState ((=) "Active") namespaceName
                logger.Logf LogLevel.Info "Created new default MBrace Service Bus namespace %s" namespaceName
                return namespaceName
        }

        return! retryAsync (RetryPolicy.Retry(maxRetries = 3, delay = 0.3<sec>)) (aux())
    }

    let getNamespaces (client:SubscriptionClient) = async {
        let! (nss : ServiceBusNamespacesResponse) = client.ServiceBus.Namespaces.ListAsync()
        return nss |> Seq.toArray
    }

    /// Verifies or recovers service bus account credentials
    let resolveServiceBusAccount (accountId : string) (client:SubscriptionClient) = async {
        match AzureServiceBusAccount.TryFromConnectionString accountId with
        | Some account -> return account
        | None ->
            let accountName = defaultArg (AzureServiceBusAccount.TryParseNamespace accountId) accountId
            // input identifier as account name, recover connection string from Storage Account client
            let! (authRules : ServiceBusAuthorizationRulesResponse) = client.ServiceBus.Namespaces.ListAuthorizationRulesAsync accountName
            let rootSharedAccessKey = authRules |> Seq.find (fun rule -> rule.KeyName = "RootManageSharedAccessKey")
            return AzureServiceBusAccount.FromCredentials(accountName, rootSharedAccessKey.PrimaryKey)
    }

//    let findOrCreateMBraceNamespace (logger : ISystemLogger) region client = async {
//        let! nsOpt = tryFindMBraceNamespace region client
//        match nsOpt with
//        | Some ns -> 
//            logger.Logf LogLevel.Info "Reusing existing Service Bus namespace %A" ns
//            return ns
//        | None -> 
//            let namespaceName = Common.generateResourceName()
//            return! createNamespace logger region namespaceName client
//    }

    /// Verifies or creates a new service bus account for provided deployment
    let getDeploymentServiceBusAccount (logger : ISystemLogger) (region : Region) (serviceBusId : string option) (client : SubscriptionClient) = async {
        match serviceBusId with
        | Some id -> 
            // parse or validate user-supplied service bus account
            let! account = resolveServiceBusAccount id client
            logger.Logf LogLevel.Info "using user-supplied service bus account %A" account.AccountName
            return account

        | None ->
            // no account specified, create a new one or reuse existing
            // we only reuse storage acounts that are not part of current active deployments
            let! accountsT = listLocalMBraceServiceBusAccounts region client |> Async.StartChild
            let! clusters = Compute.getRunningDeployments client
            let! accounts = accountsT

            let activeAccounts = clusters |> Seq.map (fun dI -> dI.Configuration.ServiceBusAccount) |> set
            match accounts |> Array.tryFind (not << activeAccounts.Contains) with
            | Some inactiveAccount -> 
                logger.Logf LogLevel.Info "reusing inactive service bus account %A" inactiveAccount
                return! resolveServiceBusAccount inactiveAccount client

            | None ->
                // no inactive service bus account, automatically create a new one
                let accountName = Common.generateResourceName()
                logger.Logf LogLevel.Info "creating new service bus account %A" accountName
                let! accountName = createServiceBusAccount logger region accountName client
                return! resolveServiceBusAccount accountName client 
    }

    /// Asynchronously deletes Azure service bus account
    let deleteServiceBusAccount namespaceName (client:SubscriptionClient) = async {
        let! _response = client.ServiceBus.Namespaces.DeleteAsync namespaceName
        do! client |> waitUntilState ((<>) "Removing") namespaceName
    }