namespace MBrace.Azure.Management

open System

open MBrace.Core.Internals
open MBrace.Runtime

open Microsoft.WindowsAzure.Management.ServiceBus
open Microsoft.WindowsAzure.Management.ServiceBus.Models
open MBrace.Azure.Runtime

module internal ServiceBus =

    let tryParseEndpoint (endpoint : string) =
        let ok, uri = Uri.TryCreate(endpoint, UriKind.Absolute)
        if ok && uri.Scheme = "sb" then Some (uri.Host.Split('.').[0])
        else None

    let rec waitUntilState checkState ns (client:SubscriptionClient) = async {
        let! result = client.ServiceBus.Namespaces.GetAsync ns |> Async.AwaitTaskCorrect |> Async.Catch
        let status = match result with Choice1Of2 ns -> ns.Namespace.Status | Choice2Of2 _ -> ""
        if not (checkState status) then
            do! Async.Sleep 2000
            return! waitUntilState checkState ns client
    }

    let rec createNamespace (logger : ISystemLogger) (region : Region) (namespaceName : string) (client:SubscriptionClient) = async {
        logger.Logf LogLevel.Info "checking availability of service bus namespace %s" namespaceName
        let! (availability : CheckNamespaceAvailabilityResponse) = client.ServiceBus.Namespaces.CheckAvailabilityAsync namespaceName
        if not availability.IsAvailable then 
            return! createNamespace logger region namespaceName client
        else
            logger.Logf LogLevel.Info "creating service bus namespace %s" namespaceName
            let result = client.ServiceBus.Namespaces.Create(namespaceName, region.Id) 

            do! client |> waitUntilState ((=) "Active") namespaceName

            if result.StatusCode <> Net.HttpStatusCode.OK then 
                return failwithf "Failed to create service bus: %O" result.StatusCode
            else 
                logger.Logf LogLevel.Info "Created new default MBrace Service Bus namespace %s" namespaceName
                return namespaceName 
    }

    let getNamespaces (client:SubscriptionClient) = async {
        let! (nss : ServiceBusNamespacesResponse) = client.ServiceBus.Namespaces.ListAsync()
        return nss |> Seq.toArray
    }

    let tryFindMBraceNamespace (region : Region) (client:SubscriptionClient) = async {
        let! accounts = getNamespaces client
        return
            accounts
            |> Seq.filter (fun ns -> ns.Region = region.Id)
            |> Seq.filter(fun ns -> ns.Name.StartsWith Common.resourcePrefix)
            |> Seq.map(fun ns -> ns.Name)
            |> Seq.tryPick Some
    }

    let getAccount (accountId : string) (client:SubscriptionClient) = async {
        match AzureServiceBusAccount.TryFromConnectionString accountId with
        | Some account -> return account
        | None ->
            let accountName = defaultArg (tryParseEndpoint accountId) accountId
            // input identifier as account name, recover connection string from Storage Account client
            let! (authRules : ServiceBusAuthorizationRulesResponse) = client.ServiceBus.Namespaces.ListAuthorizationRulesAsync accountName
            let rootSharedAccessKey = authRules |> Seq.find (fun rule -> rule.KeyName = "RootManageSharedAccessKey")
            return AzureServiceBusAccount.FromCredentials(accountName, rootSharedAccessKey.PrimaryKey)
    }

    let findOrCreateMBraceNamespace (logger : ISystemLogger) region client = async {
        let! nsOpt = tryFindMBraceNamespace region client
        match nsOpt with
        | Some ns -> 
            logger.Logf LogLevel.Info "Reusing existing Service Bus namespace %A" ns
            return ns
        | None -> 
            let namespaceName = Common.generateResourceName()
            return! createNamespace logger region namespaceName client
    }

    let resolveNamespaceInfo (logger : ISystemLogger) (region : Region) (serviceBusId : string option) (client : SubscriptionClient) = async {
        match serviceBusId with
        | Some id -> return! getAccount id client
        | None ->
            let! ns = findOrCreateMBraceNamespace logger region client
            return! getAccount ns client    
    }

    let deleteNamespace namespaceName (client:SubscriptionClient) = async {
        let! _response = client.ServiceBus.Namespaces.DeleteAsync namespaceName
        do! client |> waitUntilState ((<>) "Removing") namespaceName
    }