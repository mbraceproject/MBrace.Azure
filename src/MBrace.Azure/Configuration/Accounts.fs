namespace MBrace.Azure.Runtime

open System
open System.Collections.Concurrent
open System.Runtime.Serialization
open System.Text.RegularExpressions

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

    static let mkConnectionString (name : string) (key : string) =
        sprintf "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s" name key

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
    /// Base-64 Account key representation
    member __.AccountKey = getLocalData().Account.Credentials.ExportBase64EncodedKey()
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
    static member TryFromConnectionString(connectionString : string) =
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
                    AccountName = account.Credentials.AccountName
                    Account = account
                    ConnectionString = connectionString
                    BlobClient = blobClient
                    TableClient = tableClient
                }

            let data = localContainer.GetOrAdd(data.AccountName, data)
            Some(new AzureStorageAccount(data))
        else
            None

    /// <summary>
    ///     Creates an Azure storage account reference using provided connection string.
    /// </summary>
    /// <param name="connectionString">Azure Storage account connection string.</param>
    static member FromConnectionString(connectionString : string) =
        match AzureStorageAccount.TryFromConnectionString connectionString with
        | None -> invalidArg "connectionString" (sprintf "Invalid Storage connection string '%s'" connectionString)
        | Some asa -> asa

    /// <summary>
    ///     Creates an Azure storage account reference using provided credentials.
    /// </summary>
    /// <param name="accountName">Storage account name.</param>
    /// <param name="accountKey">Base-64 storage account key.</param>
    static member FromCredentials(accountName : string, accountKey : string) =
        let connectionString = mkConnectionString accountName accountKey
        AzureStorageAccount.FromConnectionString(connectionString)

[<AutoSerializable(false); NoEquality; NoComparison>]
type private ServiceBusAccountData = 
    { 
        ConnectionString : string
        AccountNameOrNamespace : string
        AccountKey : string
        NamespaceManager : NamespaceManager
    }

/// Azure ServiceBus Account reference that does not leak connection string information to its serialization.
[<Sealed; DataContract; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type AzureServiceBusAccount private (data: ServiceBusAccountData) =
    static let localContainer = new ConcurrentDictionary<string, ServiceBusAccountData> ()
    static let mkNamespace accountName = sprintf "sb://%s.servicebus.windows.net/" accountName
    static let namespaceRegex = new Regex("sb://(.+)\.servicebus\.windows\.net/", RegexOptions.Compiled)
    static let connectionStringRegex = 
        new Regex("Endpoint=(.+);SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=(.+)", RegexOptions.Compiled)

    static let mkConnectionString endpoint key =
        sprintf "EndPoint=%s;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=%s" endpoint key

    static let tryParse(connectionString : string) =
        try
            let nsm = NamespaceManager.CreateFromConnectionString connectionString
            let _ = nsm.QueueExists "foobar" // force exception here in case of invalid connection string
            let accountNameOrNamespace =
                let ns = nsm.Address.ToString()
                let m = namespaceRegex.Match ns
                if m.Success then
                    m.Groups.[1].Value
                else
                    ns

            let accountKey = connectionStringRegex.Match(connectionString).Groups.[2].Value
            
            Choice1Of2 { 
                ConnectionString = connectionString
                AccountNameOrNamespace = accountNameOrNamespace
                AccountKey = accountKey
                NamespaceManager = nsm 
            }

        with e -> Choice2Of2 e

    [<DataMember(Name = "AccountName")>]
    let accountName = data.AccountNameOrNamespace

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
                invalidOp <| sprintf "Could not resolve Azure service bus account '%s' from current process." accountName

    /// Account name or namespace identifier
    member __.AccountName = accountName
    /// Base-64 account key representation
    member __.AccountKey = getData().AccountKey
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
            | :? AzureServiceBusAccount as asba -> compare accountName asba.AccountName
            | _ -> invalidArg "other" "invalid comparand."

    override __.Equals(other : obj) =
        match other with
        | :? AzureServiceBusAccount as asba -> accountName = asba.AccountName
        | _ -> false

    override __.GetHashCode() = hash accountName

    member private __.StructuredFormatDisplay = sprintf "Azure Storage Account {%s}" accountName
    override __.ToString() = __.StructuredFormatDisplay

    /// <summary>
    ///     Try creating an Azure service bus account reference using provided connection string.
    /// </summary>
    /// <param name="connectionString">Azure service bus connection string.</param>
    static member TryFromConnectionString(connectionString : string) =
        match tryParse connectionString with
        | Choice2Of2 _ -> None
        | Choice1Of2 data ->
            let data = localContainer.GetOrAdd(data.AccountNameOrNamespace, data)
            Some(new AzureServiceBusAccount(data))

    /// <summary>
    ///     Creates an Azure service bus account reference using provided connection string.
    /// </summary>
    /// <param name="connectionString">Azure service bus connection string.</param>
    static member FromConnectionString(connectionString : string) =
        match tryParse connectionString with
        | Choice2Of2 e -> raise <| new ArgumentException(sprintf "Invalid ServiceBus connection string '%s'." connectionString, "connectionString", e)
        | Choice1Of2 data ->
            let data = localContainer.GetOrAdd(data.AccountNameOrNamespace, data)
            new AzureServiceBusAccount(data)

    /// <summary>
    ///     Creates an Azure service bus account reference using provided credentials.
    /// </summary>
    /// <param name="accountNameOrNamespace">Service bus account name or namespace identifier.</param>
    /// <param name="accountKey">Base-64 service bus account key.</param>
    static member FromCredentials(accountNameOrNamespace : string, accountKey : string) =
        let nameSpace =
            if namespaceRegex.IsMatch accountNameOrNamespace then accountNameOrNamespace
            else mkNamespace accountNameOrNamespace

        let conn = mkConnectionString nameSpace accountKey
        AzureServiceBusAccount.FromConnectionString conn

    /// <summary>
    ///     Attempt to parse service bus namespaces of format 'sb://&lt;account&gt;.servicebus.windows.net/',
    ///     returning the account name.
    /// </summary>
    /// <param name="servicebusNamespace">Input service bus namespace</param>
    static member TryParseNamespace(servicebusNamespace : string) =
        let m = namespaceRegex.Match servicebusNamespace
        if m.Success then
            let accountName = m.Groups.[1].Value
            Some accountName
        else
            None