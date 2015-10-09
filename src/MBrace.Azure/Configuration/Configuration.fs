namespace MBrace.Azure

open System

open MBrace.Azure.Runtime

/// Azure Configuration Builder. Used to specify MBrace.Azure cluster storage configuration.
[<AutoSerializable(true); Sealed; NoEquality; NoComparison>]
type Configuration(storageConnectionString : string, serviceBusConnectionString : string) =

    static let getEnv (envName:string) =
        let aux found target =
            if String.IsNullOrWhiteSpace found then Environment.GetEnvironmentVariable(envName, target)
            else found

        Array.fold aux null [|EnvironmentVariableTarget.Process; EnvironmentVariableTarget.User; EnvironmentVariableTarget.Machine|]        

    let mutable _storageAccountName = null
    let mutable _storageConnectionString = null

    let mutable _serviceBusAccountName = null
    let mutable _serviceBusConnectionString = null

    let parseStorage conn =
        let account = AzureStorageAccount.Parse conn
        _storageConnectionString <- account.ConnectionString
        _storageAccountName <- account.AccountName

    let parseServiceBus conn =
        let account = AzureServiceBusAccount.Parse conn
        _serviceBusConnectionString <- account.ConnectionString
        _serviceBusAccountName <- account.AccountName

    do
        parseStorage storageConnectionString
        parseServiceBus serviceBusConnectionString

    let mutable version = typeof<Configuration>.Assembly.GetName().Version

    // Default Service Bus Configuration
    let mutable runtimeQueue        = "MBraceQueue"
    let mutable runtimeTopic        = "MBraceTopic"

    // Default Blob Storage Containers
    let mutable runtimeContainer    = "mbraceruntimedata"
    let mutable userDataContainer   = "mbraceuserdata"
    let mutable cloudValueContainer = "cloudvalue"
    let mutable assemblyContainer   = "vagabond"

    // Default Table Storage tables
    let mutable userDataTable       = "MBraceUserData"
    let mutable runtimeTable        = "MBraceRuntimeData"
    let mutable runtimeLogsTable    = "MBraceRuntimeLogs"

    /// Runtime version this configuration is targeting. Default to current assembly version.
    member __.Version
        with get () = version.ToString()
        and set v = version <- Version.Parse v

    /// Append version to given configuration e.g. $RuntimeQueue$Version. Defaults to true.
    member val UseVersionSuffix    = true with get, set

    /// Runtime identifier, used for runtime isolation when using the same storage/servicebus accounts. Defaults to 0.
    member val SuffixId             = 0us with get, set

    /// Append runtime id to given configuration e.g. $RuntimeQueue$Version$Id. Defaults to true.
    member val UseSuffixId          = true with get, set

    /// Specifies wether the cluster should optimize closure serialization. Defaults to true.
    member val OptimizeClosureSerialization = true with get, set

    // #region Credentials

    /// Azure Storage account name
    member __.StorageAccount = _storageAccountName
    /// Azure ServiceBus account name
    member __.ServiceBusAccount = _serviceBusAccountName

    /// Azure Storage connection string.
    member __.StorageConnectionString
        with get () = _storageConnectionString
        and set scs = parseStorage scs

    /// Azure Service Bus connection string.
    member __.ServiceBusConnectionString
        with get () = serviceBusConnectionString
        and set sbcs = parseServiceBus sbcs


    // #region Service Bus

    /// Service Bus queue used by the runtime.
    member __.RuntimeQueue
        with get () = runtimeQueue
        and set rq = Validate.queueName rq ; runtimeQueue <- rq

    /// Service Bus topic used by the runtime.
    member __.RuntimeTopic
        with get () = runtimeTopic
        and set rt = Validate.queueName rt ; runtimeTopic <- rt


    // #region Blob Storage


    /// Azure Storage container used by the runtime.
    member __.RuntimeContainer
        with get () = runtimeContainer
        and set rc = Validate.containerName rc ; runtimeContainer <- rc

    /// Azure Storage container used for user data.
    member __.UserDataContainer
        with get () = userDataContainer
        and set udc = Validate.containerName udc ; userDataContainer <- udc

    /// Azure Storage container used for Vagabond assembly dependencies.
    member __.AssemblyContainer
        with get () = assemblyContainer
        and set ac = Validate.containerName ac ; assemblyContainer <- ac

    /// Azure Storage container used for CloudValue persistence.
    member __.CloudValueContainer
        with get () = cloudValueContainer
        and set cvc = Validate.containerName cvc ; cloudValueContainer <- cvc

    // #region Table Storage

    /// Azure Storage table used by the runtime.
    member __.RuntimeTable
        with get () = runtimeTable
        and set rt = Validate.tableName rt; runtimeTable <- rt

    /// Azure Storage table used by the runtime for storing logs.
    member __.RuntimeLogsTable
        with get () = runtimeLogsTable
        and set rlt = Validate.tableName rlt ; runtimeLogsTable <- rlt

    /// Azure Storage table used for user data.
    member __.UserDataTable
        with get () = userDataTable
        and set udt = Validate.tableName udt ; userDataTable <- udt

    /// Gets or sets or the local environment Azure storage connection string
    static member EnvironmentStorageConnectionString
        with get () = 
            match getEnv "AzureStorageConnectionString" with
            | null | "" -> invalidOp "unset Azure Storage connection string environment variable."
            | conn -> conn

        and set conn =
            let _ = AzureStorageAccount.Parse conn
            Environment.SetEnvironmentVariable("AzureStorageConnectionString", conn, EnvironmentVariableTarget.User)

    /// Gets or sets or the local environment Azure service bus connection string
    static member EnvironmentServiceBusConnectionString
        with get () = 
            match getEnv "AzureServiceBusConnectionString" with
            | null | "" -> invalidOp "unset Azure ServiceBus connection string environment variable."
            | conn -> conn

        and set conn =
            let _ = AzureServiceBusAccount.Parse conn
            Environment.SetEnvironmentVariable("AzureServiceBusConnectionString", conn, EnvironmentVariableTarget.User)

    /// Creates a configuration object by reading connection string information from the local environment variables.
    static member FromEnvironmentVariables() =
        new Configuration(Configuration.EnvironmentStorageConnectionString, Configuration.EnvironmentServiceBusConnectionString)