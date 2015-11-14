namespace MBrace.Azure.Management

open Microsoft.Azure
open Microsoft.WindowsAzure.Management.Storage
open Microsoft.WindowsAzure.Management.Storage.Models

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils.Retry
open MBrace.Azure.Runtime

module internal Storage =

    let listAllStorageAccounts (client:SubscriptionClient) = async {
        let! (listed : StorageAccountListResponse) = client.Storage.StorageAccounts.ListAsync()
        return listed |> Seq.toArray
    }

    let listLocalMBraceStorageAccounts (region : Region) (client:SubscriptionClient) = async {
        let! accounts = listAllStorageAccounts client
        return
            accounts
            |> Seq.filter (fun account -> 
                let hasLocationData, storageAccountLocation = account.ExtendedProperties.TryGetValue "ResourceLocation"
                hasLocationData && storageAccountLocation = region.Id)
            |> Seq.filter(fun account -> account.ExtendedProperties |> Common.isMBraceAsset)
            |> Seq.map(fun account -> account.Name)
            |> Seq.toArray
    }

    /// Attempt to create an Azure storage account for usage by MBrace
    let createMBraceStorageAccount (logger : ISystemLogger) (region : Region) (accountName : string) (client:SubscriptionClient) = async {
        let aux () = async {
            let! (availability : CheckNameAvailabilityResponse) = client.Storage.StorageAccounts.CheckNameAvailabilityAsync accountName
            if not availability.IsAvailable then 
                return invalidOp <| sprintf "Storage account name %A is not available" accountName

            let storageParams = new StorageAccountCreateParameters(Name = accountName, AccountType = "Standard_LRS", Location = region.Id, ExtendedProperties = Common.defaultExtendedProperties)
            let! (result : OperationStatusResponse) = client.Storage.StorageAccounts.CreateAsync(storageParams)
            if result.Status = OperationStatus.Failed then 
                return invalidOp <| sprintf "Error creating storage account %A : %s" accountName result.Error.Message

            logger.Logf LogLevel.Info "Created new storage account %A" accountName
            return accountName
        }   

        return! retryAsync (RetryPolicy.Retry(maxRetries = 3, delay = 0.3<sec>)) (aux ())
    }

    /// Resolves storage account auth info of given id
    let resolveStorageAccount (accountId : string) (client:SubscriptionClient) = async {
        match AzureStorageAccount.TryFromConnectionString accountId with
        | Some account -> return account
        | None ->
            // input identifier as account name, recover connection string from Storage Account client
            let! (keys : StorageAccountGetKeysResponse) = client.Storage.StorageAccounts.GetKeysAsync accountId
            return AzureStorageAccount.FromCredentials(accountId, keys.PrimaryKey)
    }

    /// Creates or resolves supplied storage account for an Azure deployment
    let getDeploymentStorageAccount (logger : ISystemLogger) (region : Region) (storageAccount : string option) (client : SubscriptionClient) = async {
        match storageAccount with
        | Some account -> 
            // parse and validate storage account info
            let! account = resolveStorageAccount account client
            logger.Logf LogLevel.Info "using user-supplied storage account %A" account.AccountName
            return account

        | None ->
            // no account specified, create a new one or reuse existing
            // we only reuse storage acounts that are not part of current active deployments
            let! accountsT = listLocalMBraceStorageAccounts region client |> Async.StartChild
            let! clusters = Compute.getRunningDeployments client
            let! accounts = accountsT

            let activeAccounts = clusters |> Seq.map (fun dI -> dI.Configuration.StorageAccount) |> set
            match accounts |> Array.tryFind (not << activeAccounts.Contains) with
            | Some inactiveAccount -> 
                logger.Logf LogLevel.Info "reusing inactive storage account %A" inactiveAccount
                return! resolveStorageAccount inactiveAccount client

            | None ->
                // no inactive storage account, automatically create a new one
                let accountName = Common.generateResourceName()
                logger.Logf LogLevel.Info "creating new storage account %A" accountName
                let! accountName = createMBraceStorageAccount logger region accountName client
                return! resolveStorageAccount accountName client
    }