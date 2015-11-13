namespace MBrace.Azure.Management

open System
open System.Text.RegularExpressions

open MBrace.Core.Internals
open MBrace.Runtime

open Microsoft.WindowsAzure.Management.ServiceBus
open Microsoft.WindowsAzure.Management.ServiceBus.Models

module internal ServiceBus =

    let mkEndpoint (namespaceName : string) =
        sprintf "sb://%s.servicebus.windows.net/" namespaceName

    let mkConnectionString endpoint (key : string) =
        sprintf "EndPoint=%s;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=%s" endpoint key

    let private connectionStringRegex = new Regex("Endpoint=(.+);SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=(.+)", RegexOptions.Compiled)
    let tryParseConnectionString (conn : string) =
        let m = connectionStringRegex.Match(conn)
        if m.Success then
            let endPoint = m.Groups.[1].Value |> Uri
            let namespaceName = endPoint.Host.Split('.').[0]
            let accountKey = m.Groups.[2].Value
            Some(endPoint.ToString(), namespaceName, accountKey)
        else
            None

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

    let getAccountInfo (id : string) (client:SubscriptionClient) = async {
        match tryParseConnectionString id with
        | Some(endpoint, namespaceName, _) -> 
            // input identified as connection string, parse and return account name
            return namespaceName, endpoint, id
        | None ->
            let namespaceName = defaultArg (tryParseEndpoint id) id
            // input identifier as account name, recover connection string from Storage Account client
            let! (authRules : ServiceBusAuthorizationRulesResponse) = client.ServiceBus.Namespaces.ListAuthorizationRulesAsync(namespaceName)
            let rootSharedAccessKey = authRules |> Seq.find (fun rule -> rule.KeyName = "RootManageSharedAccessKey")
            let endpoint = mkEndpoint namespaceName
            let connectionString = mkConnectionString endpoint rootSharedAccessKey.PrimaryKey
            return namespaceName, endpoint, connectionString
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
        | Some id -> return! getAccountInfo id client
        | None ->
            let! ns = findOrCreateMBraceNamespace logger region client
            return! getAccountInfo ns client    
    }

    let deleteNamespace namespaceName (client:SubscriptionClient) = async {
        let! _response = client.ServiceBus.Namespaces.DeleteAsync namespaceName
        do! client |> waitUntilState ((<>) "Removing") namespaceName
    }