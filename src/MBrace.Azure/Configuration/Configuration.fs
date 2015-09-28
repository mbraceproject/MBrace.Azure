namespace MBrace.Azure

open System

open MBrace.Azure.Runtime

/// Azure Configuration Builder object
[<AutoSerializable(true); Sealed; NoEquality; NoComparison>]
type Configuration(storageConnectionString : string, serviceBusConnectionString : string) = 

    let mutable storageConnectionString = AzureStorageAccount.Parse(storageConnectionString).ConnectionString
    let mutable serviceBusConnectionString = AzureServiceBusAccount.Parse(serviceBusConnectionString).ConnectionString
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


    // #region Credentials

    /// Azure Storage connection string.
    member __.StorageConnectionString
        with get () = storageConnectionString
        and set scs = storageConnectionString <- AzureStorageAccount.Parse(scs).ConnectionString

    /// Azure Service Bus connection string.
    member __.ServiceBusConnectionString
        with get () = serviceBusConnectionString
        and set sbcs = serviceBusConnectionString <- AzureServiceBusAccount.Parse(sbcs).ConnectionString


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