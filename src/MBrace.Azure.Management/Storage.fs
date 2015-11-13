namespace MBrace.Azure.Management

open Microsoft.Azure
open Microsoft.WindowsAzure.Management.Storage
open Microsoft.WindowsAzure.Management.Storage.Models

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Azure.Runtime

module internal Storage =

    let getStorageAccounts (client:SubscriptionClient) = async {
        let! (listed : StorageAccountListResponse) = client.Storage.StorageAccounts.ListAsync()
        return listed |> Seq.toArray
    }

    let tryFindMBraceStorage (region : Region) (client:SubscriptionClient) = async {
        let! accounts = getStorageAccounts client
        return
            accounts
            |> Seq.filter (fun account -> 
                let hasLocationData, storageAccountLocation = account.ExtendedProperties.TryGetValue "ResourceLocation"
                hasLocationData && storageAccountLocation = region.Id)
            |> Seq.filter(fun account -> account.ExtendedProperties |> Common.isMBraceAsset)
            |> Seq.map(fun account -> account.Name)
            |> Seq.tryPick Some
    }

    let rec createMBraceStorageAccount (logger : ISystemLogger) (region : Region) (accountName : string) (client:SubscriptionClient) = async {
        let! (availability : CheckNameAvailabilityResponse) = client.Storage.StorageAccounts.CheckNameAvailabilityAsync accountName 
        if not availability.IsAvailable then return! createMBraceStorageAccount logger region accountName client
        else
            let storageParams = new StorageAccountCreateParameters(Name = accountName, AccountType = "Standard_LRS", Location = region.Id, ExtendedProperties = Common.defaultExtendedProperties)
            let! (result : OperationStatusResponse) = client.Storage.StorageAccounts.CreateAsync(storageParams)
            if result.Status = OperationStatus.Failed then 
                return invalidOp result.Error.Message
            else 
                logger.Logf LogLevel.Info "Created new storage account %A" accountName
                return accountName 
    }

    let getStoraceAccount (accountId : string) (client:SubscriptionClient) = async {
        match AzureStorageAccount.TryFromConnectionString accountId with
        | Some account -> return account
        | None ->
            // input identifier as account name, recover connection string from Storage Account client
            let! (keys : StorageAccountGetKeysResponse) = client.Storage.StorageAccounts.GetKeysAsync accountId
            return AzureStorageAccount.FromCredentials(accountId, keys.PrimaryKey)
    }

    let getDeploymentContainer (account : AzureStorageAccount) = async {
        let container = account.BlobClient.GetContainerReference "deployments"
        do! container.CreateIfNotExistsAsync()
        return container
    }

    let getDefaultMBraceStorageAccountName (logger : ISystemLogger) (region : Region) client = async {
        let! result = tryFindMBraceStorage region client
        match result with
        | Some storage -> 
            logger.Logf LogLevel.Info "Reusing existing storage account %s" storage
            return storage
        | None -> 
            let accountName = Common.generateResourceName()
            return! createMBraceStorageAccount logger region accountName client
    }

    let resolveStorageAccount (logger : ISystemLogger) (region : Region) (storageAccount : string option) (client : SubscriptionClient) = async {
        match storageAccount with
        | Some account -> 
            // parse and validate storage account info
            return! getStoraceAccount account client
        | None ->
            let! accountName = getDefaultMBraceStorageAccountName logger region client
            return! getStoraceAccount accountName client
    }