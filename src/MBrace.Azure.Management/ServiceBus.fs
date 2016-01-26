namespace MBrace.Azure.Management

open System

open Microsoft.WindowsAzure.Management.ServiceBus
open Microsoft.WindowsAzure.Management.ServiceBus.Models

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils.Retry
open MBrace.Runtime.Utils.PrettyPrinters
open MBrace.Azure.Runtime

module internal ServiceBus =

    let listAllServiceBusAccounts (region : Region option) (client:SubscriptionClient) = async {
        let! listed = client.ServiceBus.Namespaces.ListAsync() |> Async.AwaitTaskCorrect
        return 
            match region with
            | None -> listed |> Seq.toArray
            | Some rg -> listed |> Seq.filter (fun ac -> ac.Region = rg.Id) |> Seq.toArray
    }

    let listLocalMBraceServiceBusAccounts (region : Region) (client : SubscriptionClient) = async {
        let! accounts = listAllServiceBusAccounts (Some region) client
        return
            accounts
            |> Seq.filter(fun account -> account.Name.StartsWith Common.resourcePrefix)
            |> Seq.map(fun account -> account.Name)
            |> Seq.toArray
    }

    let waitUntilState checkState ns timeout (client:SubscriptionClient) = async {
        let rec aux () = async {
            let! result = client.ServiceBus.Namespaces.GetAsync ns |> Async.AwaitTaskCorrect |> Async.Catch
            let status = match result with Choice1Of2 ns -> ns.Namespace.Status | Choice2Of2 _ -> ""
            if not (checkState status) then
                do! Async.Sleep 2000
                return! aux ()
        }

        return! Async.WithTimeout(aux (), ?timeoutMilliseconds = timeout)
    }

    type ServiceBusAccountReporter private () =
        static let template : Field<ServiceBusNamespace> list =
            [
                Field.create "Account Name" Left (fun ns -> ns.Name)
                Field.create "Region" Left (fun ns -> ns.Region)
                Field.create "Creation Time" Left (fun ns -> ns.CreatedAt)
                Field.create "Status" Left (fun ns -> ns.Status)
                Field.create "Endpoint" Left (fun ns -> ns.ServiceBusEndpoint)
            ]

        static member Report(sbnss : ServiceBusNamespace list, ?title : string) =
            Record.PrettyPrint(template, sbnss, ?title = title, useBorders = false, parallelize = false)

    /// Creates a new service bus account in supplied region
    let createServiceBusAccount (logger : ISystemLogger) (region : Region) (namespaceName : string) (client:SubscriptionClient) = async {
        let aux () = async {
            let! availability = client.ServiceBus.Namespaces.CheckAvailabilityAsync namespaceName |> Async.AwaitTaskCorrect
            if not availability.IsAvailable then
                return invalidOp <| sprintf "ServiceBus namespace name %A is not available" namespaceName

            let! result = client.ServiceBus.Namespaces.CreateAsync(namespaceName, region.Id) |> Async.AwaitTaskCorrect
            if result.StatusCode <> Net.HttpStatusCode.OK then 
                return invalidOp <| sprintf "Failed to create service bus: %O" result.StatusCode
            else 
                do! client |> waitUntilState ((=) "Active") namespaceName (Some 60000)
                logger.Logf LogLevel.Info "Created new default MBrace Service Bus namespace %s" namespaceName
                return namespaceName
        }

        return! retryAsync (RetryPolicy.Retry(maxRetries = 3, delay = 0.3<sec>)) (aux())
    }

    /// Verifies or recovers service bus account credentials
    let resolveServiceBusAccount (logger : ISystemLogger) (verify : Region option) (accountId : string) (client:SubscriptionClient) = async {
        let! account = async {
            match AzureServiceBusAccount.TryFromConnectionString accountId with
            | Some account -> return new ServiceBusAccount(account)
            | None ->
                let accountName = defaultArg (AzureServiceBusAccount.TryParseNamespace accountId) accountId
                // input identifier as account name, recover connection string from Storage Account client
                let! authRules = client.ServiceBus.Namespaces.ListAuthorizationRulesAsync accountName |> Async.AwaitTaskCorrect
                let rootSharedAccessKey = authRules |> Seq.find (fun rule -> rule.KeyName = "RootManageSharedAccessKey")
                let account = AzureServiceBusAccount.FromCredentials(accountName, rootSharedAccessKey.PrimaryKey)
                return new ServiceBusAccount(account)
        }

        match verify with
        | None -> ()
        | Some region ->
            try
                let! info = client.ServiceBus.Namespaces.GetAsync account.AccountName |> Async.AwaitTaskCorrect
                if info.StatusCode <> System.Net.HttpStatusCode.OK then
                    logger.Logf LogLevel.Warning "Service Bus account %A does not correspond to subscription %A" account.AccountName client.Subscription.Name
                elif info.Namespace.Region <> region.Id then
                    logger.Logf LogLevel.Warning "Service Bus account %A does not correspond to region %A. Please consider using a collocated service bus account." account.AccountName region.Id

            with _ ->
                logger.Logf LogLevel.Warning "Service Bus account %A does not correspond to subscription %A" account.AccountName client.Subscription.Name

        return account

    }

    /// Verifies or creates a new service bus account for provided deployment
    let getDeploymentServiceBusAccount (logger : ISystemLogger) (reuse : bool) (region : Region) (serviceBusId : string option) (client : SubscriptionClient) = async {
        let mkNewServiceBusAccount() = async {
            let accountName = Common.generateResourceName()
            logger.Logf LogLevel.Info "creating new service bus account %A" accountName
            let! accountName = createServiceBusAccount logger region accountName client
            return! resolveServiceBusAccount logger None accountName client 
        }

        match serviceBusId with
        | Some id -> 
            // parse or validate user-supplied service bus account
            let! account = resolveServiceBusAccount logger (Some region) id client
            logger.Logf LogLevel.Info "using user-supplied service bus account %A" account.AccountName
            return account

        | None when not reuse -> return! mkNewServiceBusAccount()
        | None ->
            // no account specified, create a new one or reuse existing
            // we only reuse storage acounts that are not part of current active deployments
            let! accountsT = listLocalMBraceServiceBusAccounts region client |> Async.StartChild
            let! clusters = Compute.getRunningDeployments client
            let! accounts = accountsT

            let activeAccounts = clusters |> Seq.map (fun dI -> dI.ServiceBusAccount.AccountName) |> set
            match accounts |> Array.tryFind (not << activeAccounts.Contains) with
            | Some inactiveAccount when reuse -> 
                logger.Logf LogLevel.Info "reusing inactive service bus account %A" inactiveAccount
                return! resolveServiceBusAccount logger None inactiveAccount client

            | _ ->
                // no inactive service bus account, automatically create a new one
                return! mkNewServiceBusAccount()
    }

    /// Asynchronously deletes Azure service bus account
    let deleteServiceBusAccount (logger : ISystemLogger) namespaceName (client:SubscriptionClient) = async {
        logger.Logf LogLevel.Info "Deleting service bus account %A" namespaceName
        let! _response = client.ServiceBus.Namespaces.DeleteAsync namespaceName |> Async.AwaitTaskCorrect
        do! client |> waitUntilState ((<>) "Removing") namespaceName (Some 60000)
    }