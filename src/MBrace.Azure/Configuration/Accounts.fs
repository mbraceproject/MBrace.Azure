namespace MBrace.Azure.Runtime

open System
open System.Collections.Concurrent
open System.Runtime.Serialization

open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Microsoft.WindowsAzure.Storage.Blob
open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging

[<AutoSerializable(false); NoEquality; NoComparison>]
type private AzureStorageAccountData = 
    { 
        AccountName : string
        ConnectionString : string
        Account : CloudStorageAccount
        TableClient : CloudTableClient
        BlobClient : CloudBlobClient
    }

/// Azure Storace Account reference that does not leak connection string information to its serialization.
[<Sealed; DataContract; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type AzureStorageAccount private (data : AzureStorageAccountData) =
    static let localContainer = new ConcurrentDictionary<string, AzureStorageAccountData> ()

    [<DataMember(Name = "AccountName")>]
    let accountName = data.AccountName

    [<IgnoreDataMember>]
    let mutable localData : AzureStorageAccountData option = Some data

    let getLocalData () =
        match localData with
        | Some ld -> ld
        | None ->
            let mutable ld = Unchecked.defaultof<_>
            if localContainer.TryGetValue(accountName, &ld) then
                localData <- Some ld
                ld
            else
                invalidOp <| sprintf "Could not resolve Azure storage account '%s' from current process." accountName

    /// Account name identifier
    member __.AccountName = accountName
    /// Account Connection string
    member __.ConnectionString = getLocalData().ConnectionString
    /// Azure account object
    member __.CloudStorageAccount = getLocalData().Account
    /// Azure table client object
    member __.TableClient = getLocalData().TableClient
    /// Azure blob client object
    member __.BlobClient = getLocalData().BlobClient

    /// Creates a table reference for given name
    member __.GetTableReference(tableName : string) = __.TableClient.GetTableReference(tableName)
    /// Creates a container reference for given name
    member __.GetContainerReference(container : string) = __.BlobClient.GetContainerReference(container)

    interface IComparable with
        member __.CompareTo(other : obj) =
            match other with
            | :? AzureStorageAccount as asa -> compare accountName asa.AccountName
            | _ -> invalidArg "other" "invalid comparand."

    override __.Equals(other : obj) =
        match other with
        | :? AzureStorageAccount as asa -> accountName = asa.AccountName
        | _ -> false

    override __.GetHashCode() = hash accountName

    member private __.StructuredFormatDisplay = sprintf "Azure Storage Account {%s}" accountName
    override __.ToString() = __.StructuredFormatDisplay

    /// <summary>
    ///     Try creating an Azure storage account reference using provided connection string.
    /// </summary>
    /// <param name="connectionString">Azure Storage account connection string.</param>
    static member TryParse(connectionString : string) =
        let mutable account = Unchecked.defaultof<_>
        if CloudStorageAccount.TryParse(connectionString, &account) then
            // init table client
            let tableClient = account.CreateCloudTableClient()
            tableClient.DefaultRequestOptions.RetryPolicy <- RetryPolicies.ExponentialRetry(TimeSpan.FromSeconds(3.), 10)
            // init blob client
            let blobClient = account.CreateCloudBlobClient()
            blobClient.DefaultRequestOptions.ParallelOperationThreadCount <- System.Nullable(min 64 (4 * System.Environment.ProcessorCount))
            blobClient.DefaultRequestOptions.SingleBlobUploadThresholdInBytes <- System.Nullable(1L <<< 23) // 8MB, possible ranges: 1..64MB, default 32MB
            blobClient.DefaultRequestOptions.MaximumExecutionTime <- Nullable<_>(TimeSpan.FromMinutes(20.))
            blobClient.DefaultRequestOptions.RetryPolicy <- RetryPolicies.ExponentialRetry(TimeSpan.FromSeconds(3.), 10)
            // create local data record
            let data = 
                { 
                    AccountName = account.Credentials.AccountName ; Account = account ; ConnectionString = connectionString 
                    BlobClient = blobClient ; TableClient = tableClient    
                }

            let data = localContainer.GetOrAdd(data.AccountName, data)
            Some(new AzureStorageAccount(data))
        else
            None

    /// <summary>
    ///     Creates an Azure storage account reference using provided connection string.
    /// </summary>
    /// <param name="connectionString">Azure Storage account connection string.</param>
    static member Parse(connectionString : string) =
        match AzureStorageAccount.TryParse connectionString with
        | None -> raise <| InvalidConfigurationException(sprintf "Invalid Storage connection string '%s'" connectionString)
        | Some asa -> asa

[<AutoSerializable(false); NoEquality; NoComparison>]
type private ServiceBusAccountData = 
    { 
        ConnectionString : string
        AccountName : string
        NamespaceManager : NamespaceManager
    }

/// Azure ServiceBus Account reference that does not leak connection string information to its serialization.
[<Sealed; DataContract; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type AzureServiceBusAccount private (data: ServiceBusAccountData) =
    static let localContainer = new ConcurrentDictionary<string, ServiceBusAccountData> ()
    static let tryParse(connectionString : string) =
        try
            let nsm = NamespaceManager.CreateFromConnectionString connectionString
            let _ = nsm.QueueExists "foobar" // force exception here in case of invalid connection string
            let accountName = nsm.Address.ToString()
            Choice1Of2 { ConnectionString = connectionString ; AccountName = accountName ; NamespaceManager = nsm }
        with e -> Choice2Of2 e

    [<DataMember(Name = "accountName")>]
    let accountName = data.AccountName

    [<IgnoreDataMember>]
    let mutable localData : ServiceBusAccountData option = Some data

    let getData() =
        match localData with
        | Some ld -> ld
        | None ->
            let mutable data = Unchecked.defaultof<_>
            if localContainer.TryGetValue(accountName, &data) then
                localData <- Some data
                data
            else
                invalidOp <| sprintf "Could not resolve Azure storage account '%s' from current process." accountName

    /// Account identifier
    member __.AccountName = accountName
    /// Namespace manager for Service Bus
    member __.NamespaceManager = getData().NamespaceManager
    /// Service bus connection string
    member __.ConnectionString = getData().ConnectionString
    /// Creates a Queue client instance
    member __.CreateQueueClient(queue : string, mode : ReceiveMode) = QueueClient.CreateFromConnectionString(__.ConnectionString, queue, mode)
    /// Creates a Subscription instance
    member __.CreateSubscriptionClient(topic : string, name : string) = SubscriptionClient.CreateFromConnectionString(__.ConnectionString, topic, name)
    /// Creates a Topic client
    member __.CreateTopicClient(topic : string) = TopicClient.CreateFromConnectionString(__.ConnectionString, topic)

    interface IComparable with
        member __.CompareTo(other : obj) =
            match other with
            | :? AzureStorageAccount as asba -> compare accountName asba.AccountName
            | _ -> invalidArg "other" "invalid comparand."

    override __.Equals(other : obj) =
        match other with
        | :? AzureStorageAccount as asba -> accountName = asba.AccountName
        | _ -> false

    override __.GetHashCode() = hash accountName

    member private __.StructuredFormatDisplay = sprintf "Azure Storage Account {%s}" accountName
    override __.ToString() = __.StructuredFormatDisplay

    /// <summary>
    ///     Try creating an Azure service bus account reference using provided connection string.
    /// </summary>
    /// <param name="connectionString">Azure service bus connection string.</param>
    static member TryParse(connectionString : string) =
        match tryParse connectionString with
        | Choice2Of2 _ -> None
        | Choice1Of2 data ->
            let data = localContainer.GetOrAdd(data.AccountName, data)
            Some(new AzureServiceBusAccount(data))

    /// <summary>
    ///     Creates an Azure service bus account reference using provided connection string.
    /// </summary>
    /// <param name="connectionString">Azure service bus connection string.</param>
    static member Parse(connectionString : string) =
        match tryParse connectionString with
        | Choice2Of2 e -> raise <| InvalidConfigurationException(sprintf "Invalid ServiceBus connection string '%s'." connectionString, e)
        | Choice1Of2 data ->
            let data = localContainer.GetOrAdd(data.AccountName, data)
            new AzureServiceBusAccount(data)