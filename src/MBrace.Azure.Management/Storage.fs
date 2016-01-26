namespace MBrace.Azure.Management

open Microsoft.Azure
open Microsoft.WindowsAzure.Management.Storage
open Microsoft.WindowsAzure.Management.Storage.Models

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils.Retry
open MBrace.Runtime.Utils.PrettyPrinters
open MBrace.Azure.Runtime

module internal Storage =

    type MStorageAccount = MBrace.Azure.Management.StorageAccount

    let tryGetRegion (acc : StorageAccount) =
        let ok, location = acc.ExtendedProperties.TryGetValue "ResourceLocation"
        if ok then Some location
        else None

    let listAllStorageAccounts (region : Region option) (client:SubscriptionClient) = async {
        let! listed = client.Storage.StorageAccounts.ListAsync() |> Async.AwaitTaskCorrect
        return 
            match region with 
            | None -> listed |> Seq.toArray
            | Some rg -> listed |> Seq.filter (fun account -> tryGetRegion account |> Option.exists (fun r -> r = rg.Id)) |> Seq.toArray
    }

    let listLocalMBraceStorageAccounts (region : Region) (client:SubscriptionClient) = async {
        let! accounts = listAllStorageAccounts (Some region) client
        return
            accounts
            |> Seq.filter(fun account -> account.ExtendedProperties |> Common.isMBraceAsset)
            |> Seq.map(fun account -> account.Name)
            |> Seq.toArray
    }

    type StorageAccountReporter private () =
        static let template : Field<StorageAccount> list =
            [
                Field.create "Account Name" Left (fun sa -> sa.Name)
                Field.create "Region" Left (fun sa -> defaultArg (tryGetRegion sa) "?")
                Field.create "Account Type" Left (fun sa -> sa.Properties.AccountType)
                Field.create "Status" Left (fun sa -> sa.Properties.Status)
                Field.create "Affinity Group" Left (fun sa -> match sa.Properties.AffinityGroup with null -> "N/A" | ag -> ag)
            ]

        static member Report(sbnss : StorageAccount list, ?title : string) =
            Record.PrettyPrint(template, sbnss, ?title = title, useBorders = false, parallelize = false)

    /// Attempt to create an Azure storage account for usage by MBrace
    let createMBraceStorageAccount (logger : ISystemLogger) (region : Region) (accountName : string) (client:SubscriptionClient) = async {
        let aux () = async {
            let! availability = client.Storage.StorageAccounts.CheckNameAvailabilityAsync accountName |> Async.AwaitTaskCorrect
            if not availability.IsAvailable then 
                return invalidOp <| sprintf "Storage account name %A is not available" accountName

            let storageParams = new StorageAccountCreateParameters(Name = accountName, AccountType = "Standard_LRS", Location = region.Id, ExtendedProperties = Common.defaultExtendedProperties)
            let! result = client.Storage.StorageAccounts.CreateAsync(storageParams) |> Async.AwaitTaskCorrect
            if result.Status = OperationStatus.Failed then 
                return invalidOp <| sprintf "Error creating storage account %A : %s" accountName result.Error.Message

            logger.Logf LogLevel.Info "Created new storage account %A" accountName
            return accountName
        }   

        return! retryAsync (RetryPolicy.Retry(maxRetries = 3, delay = 0.3<sec>)) (aux ())
    }

    let deleteStorageAccount (logger : ISystemLogger) (accountName : string) (client : SubscriptionClient) = async {
        logger.Logf LogLevel.Info "Deleting storage account %A" accountName
        let! response = client.Storage.StorageAccounts.DeleteAsync accountName |> Async.AwaitTaskCorrect
        if response.StatusCode <> System.Net.HttpStatusCode.OK then
            return invalidOp <| sprintf "Error deleting storage account %A (error code %O)" accountName response.StatusCode
    }

    /// Resolves storage account auth info of given id
    let resolveStorageAccount (logger : ISystemLogger) (verify : Region option) (accountId : string) (client:SubscriptionClient) = async {
        let! account = async {
            match AzureStorageAccount.TryFromConnectionString accountId with
            | Some account -> return account
            | None ->
                // input identifier as account name, recover connection string from Storage Account client
                let! keys = client.Storage.StorageAccounts.GetKeysAsync accountId |> Async.AwaitTaskCorrect
                let account = AzureStorageAccount.FromCredentials(accountId, keys.PrimaryKey)
                return account
        }

        match verify with
        | None -> ()
        | Some region ->
            try
                let! info = client.Storage.StorageAccounts.GetAsync account.AccountName |> Async.AwaitTaskCorrect
                if info.StatusCode <> System.Net.HttpStatusCode.OK then
                    logger.Logf LogLevel.Warning "Storage account %A does not correspond to subscription %A" account.AccountName client.Subscription.Name
                elif
                    [|  info.StorageAccount.Properties.Location
                        info.StorageAccount.Properties.GeoPrimaryRegion
                        info.StorageAccount.Properties.GeoSecondaryRegion |] |> Array.forall ((<>) region.Id) 
                then
                    logger.Logf LogLevel.Warning "Storage account %A does not correspond to region %A. Please consider using a collocated storage account." account.AccountName region.Id

            with _ ->
                logger.Logf LogLevel.Warning "Storage account %A does not correspond to subscription %A" account.AccountName client.Subscription.Name

        return new MStorageAccount(account)
    }

    /// Creates or resolves supplied storage account for an Azure deployment
    let getDeploymentStorageAccount (logger : ISystemLogger) (reuse : bool) (region : Region) (storageAccount : string option) (client : SubscriptionClient) = async {
        let mkNewStorageAccount() = async {
            // no inactive storage account, automatically create a new one
            let accountName = Common.generateResourceName()
            logger.Logf LogLevel.Info "creating new storage account %A" accountName
            let! accountName = createMBraceStorageAccount logger region accountName client
            return! resolveStorageAccount logger None accountName client
        }

        match storageAccount with
        | Some account -> 
            // parse and validate storage account info
            let! account = resolveStorageAccount logger (Some region) account client
            logger.Logf LogLevel.Info "using user-supplied storage account %A" account.AccountName
            return account

        | None when not reuse -> return! mkNewStorageAccount()
        | None ->
            // no account specified, create a new one or reuse existing
            // we only reuse storage acounts that are not part of current active deployments
            let! accountsT = listLocalMBraceStorageAccounts region client |> Async.StartChild
            let! clusters = Compute.getRunningDeployments client
            let! accounts = accountsT

            let activeAccounts = clusters |> Seq.map (fun dI -> dI.StorageAccount.AccountName) |> set
            match accounts |> Array.tryFind (not << activeAccounts.Contains) with
            | Some inactiveAccount when reuse -> 
                logger.Logf LogLevel.Info "reusing inactive storage account %A" inactiveAccount
                return! resolveStorageAccount logger None inactiveAccount client
            | _ ->
                return! mkNewStorageAccount()
    }