module MBrace.Azure.Runtime.Arguments

open System
open System.IO

open Argu

open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Runtime

/// Argu configuration schema
type private AzureArguments =
    // General-purpose arguments
    | [<AltCommandLine("-w")>] Worker_Id of id:string
    | [<AltCommandLine("-m")>] Max_Work_Items of num:int
    | Heartbeat_Interval of seconds:float
    | Heartbeat_Threshold of seconds:float
    | [<AltCommandLine("-q")>] Quiet
    | [<AltCommandLine("-L")>] Log_Level of level:int
    | [<AltCommandLine("-l")>] Log_File of path:string
    | Working_Directory of path:string
    // Connection string parameters
    | [<Mandatory>][<AltCommandLine("-s")>] Storage_Connection_String of conn:string
    | [<Mandatory>][<AltCommandLine("-b")>] Service_Bus_Connection_string of conn:string
    // Cluster configuration parameters
    | Force_Version of version:string
    | Suffix_Id of num:uint16
    | Use_Version_Suffix of bool
    | Use_Suffix_Id of bool
    | Optimize_Closure_Serialization of bool
    // ServiceBus
    | Runtime_Queue of queue_name:string
    | Runtime_Topic of topic_name:string
    // Blob Storage
    | Runtime_Container of container:string
    | Assembly_Container of container:string
    | Cloud_Value_Container of container:string
    // Table Storage
    | Runtime_Table of table:string
    | Runtime_Logs_Table of table:string
    | User_Data_Table of table:string

    interface IArgParserTemplate with
        member arg.Usage =
            match arg with
            | Quiet -> "Suppress logging to stdout by the worker."
            | Log_Level _ -> "Log level for worker system logs. Critical = 1, Error = 2, Warning = 3, Info = 4, Debug = 5. Defaults to info."
            | Log_File _ -> "Specify a log file to write worker system logs."
            | Max_Work_Items _ -> "Specify maximum number of concurrent work items."
            | Heartbeat_Interval _ -> "Specify the heartbeat interval for the worker in seconds. Defaults to 1 second."
            | Heartbeat_Threshold _ -> "Specify the heartbeat interval for the worker in seconds. Defaults to 300 seconds."
            | Working_Directory _ -> "Specify the working directory for the worker."
            | Worker_Id _ -> "Specify worker name identifier."
            | Storage_Connection_String _ -> "Azure Storage connection string."
            | Service_Bus_Connection_string _ -> "Azure ServiceBus connection string."
            | Optimize_Closure_Serialization _ -> "Specifies whether cluster should implement closure serialization optimizations. Defaults to true."
            | Force_Version _ -> "Forces an MBrace.Azure version number identifier. Defaults to compiled version."
            | Suffix_Id _ -> "User-supplied suffix identifier for Azure store resources. Defaults to 0."
            | Use_Version_Suffix _ -> "Enables or disables version suffix in store resources. Defaults to true."
            | Use_Suffix_Id _ -> "Enables or disables user-supplied suffix identifier in store resources. Defaults to true."
            | Runtime_Queue _ -> "Specifies the work item queue name in the ServiceBus."
            | Runtime_Topic _ -> "Specifies the work item topic name in the ServiceBus."
            | Runtime_Container _ -> "Specifies the blob container name used for persisting MBrace cluster data."
            | Assembly_Container _ -> "Specifies the blob container name used for persisting Assembly dependencies."
            | Cloud_Value_Container _ -> "Specifies the blob container name used for persisting CloudValue dependencies."
            | Runtime_Table _ -> "Specifies the table name used for writing MBrace cluster entries."
            | Runtime_Logs_Table _ -> "Specifies the table name used for writing MBrace cluster system log entries."
            | User_Data_Table _ -> "Specifies the table name used for writing user logs."


let private argParser = ArgumentParser.Create<AzureArguments>(errorHandler = new ProcessExiter())

/// Configuration object encoding command line parameters for an MBrace.Azure process
type ArgumentConfiguration = 
    {
        Quiet : bool
        Configuration : Configuration option
        MaxWorkItems : int option
        WorkerId : string option
        LogLevel : LogLevel option
        LogFile : string option
        HeartbeatInterval : TimeSpan option
        HeartbeatThreshold : TimeSpan option
        WorkingDirectory : string option
    }

    /// Creates a configuration object using supplied parameters.
    static member Create(?config : Configuration, ?quiet : bool, ?workingDirectory : string, ?maxWorkItems : int, ?workerId : string, ?logLevel : LogLevel, 
                            ?logfile : string, ?heartbeatInterval : TimeSpan, ?heartbeatThreshold : TimeSpan) =
        maxWorkItems |> Option.iter (fun w -> if w < 0 then invalidArg "maxWorkItems" "must be positive." elif w > 1024 then invalidArg "maxWorkItems" "exceeds 1024 limit.")
        heartbeatInterval |> Option.iter (fun i -> if i < TimeSpan.FromSeconds 1. then invalidArg "heartbeatInterval" "must be at least one second.")
        heartbeatThreshold |> Option.iter (fun i -> if i < TimeSpan.FromSeconds 1. then invalidArg "heartbeatThreshold" "must be at least one second.")
        workerId |> Option.iter Validate.subscriptionName
        let workingDirectory = workingDirectory |> Option.map Path.GetFullPath
        let quiet = defaultArg quiet false
        { Configuration = config ; MaxWorkItems = maxWorkItems ; WorkerId = workerId ; LogFile = logfile ;
            LogLevel = logLevel ; HeartbeatInterval = heartbeatInterval ; HeartbeatThreshold = heartbeatThreshold ;
            WorkingDirectory = workingDirectory ; Quiet = quiet }

    /// Converts a configuration object to a command line string.
    static member ToCommandLineArguments(cfg : ArgumentConfiguration) =
        let args = [

            match cfg.MaxWorkItems with Some w -> yield Max_Work_Items w | None -> ()
            match cfg.WorkerId with Some n -> yield Worker_Id n | None -> ()
            match cfg.LogLevel with Some l -> yield Log_Level (int l) | None -> ()
            match cfg.HeartbeatInterval with Some h -> yield Heartbeat_Interval h.TotalSeconds | None -> ()
            match cfg.HeartbeatThreshold with Some h -> yield Heartbeat_Threshold h.TotalSeconds | None -> ()
            match cfg.LogFile with Some l -> yield Log_File l | None -> ()
            match cfg.WorkingDirectory with Some w -> yield Working_Directory w | None -> ()

            match cfg.Configuration with
            | None -> ()
            | Some config ->
                yield Storage_Connection_String config.StorageConnectionString
                yield Service_Bus_Connection_string config.ServiceBusConnectionString

                yield Force_Version config.Version
                yield Suffix_Id config.SuffixId
                yield Use_Version_Suffix config.UseVersionSuffix
                yield Use_Suffix_Id config.UseSuffixId
                yield Optimize_Closure_Serialization config.OptimizeClosureSerialization

                yield Runtime_Queue config.WorkItemQueue
                yield Runtime_Topic config.WorkItemTopic

                yield Runtime_Container config.RuntimeContainer
                yield Assembly_Container config.AssemblyContainer
                yield Cloud_Value_Container config.CloudValueContainer

                yield Runtime_Table config.RuntimeTable
                yield Runtime_Logs_Table config.RuntimeLogsTable
                yield User_Data_Table config.UserDataTable
        ]

        argParser.PrintCommandLineArgumentsFlat args

    /// Parses command line arguments to a configuration object using Argu.
    static member FromCommandLineArguments(args : string []) =
        let parseResult = argParser.Parse(args)

        let maxWorkItems = parseResult.TryPostProcessResult(<@ Max_Work_Items @>, fun i -> if i < 0 then failwith "must be positive." elif i > 1024 then failwith "exceeds 1024 limit." else i)
        let quiet = parseResult.Contains <@ Quiet @>
        let logLevel = parseResult.TryPostProcessResult(<@ Log_Level @>, enum<LogLevel>)
        let logFile = parseResult.TryPostProcessResult(<@ Log_File @>, fun f -> ignore <| Path.GetFullPath f ; f) // use GetFullPath to validate chars
        let workerName = parseResult.TryPostProcessResult(<@ Worker_Id @>, fun name -> Validate.subscriptionName name; name)
        let heartbeatInterval = parseResult.TryPostProcessResult(<@ Heartbeat_Interval @>, fun i -> let t = TimeSpan.FromSeconds i in if t < TimeSpan.FromSeconds 1. then failwith "must be positive" else t)
        let heartbeatThreshold = parseResult.TryPostProcessResult(<@ Heartbeat_Threshold @>, fun i -> let t = TimeSpan.FromSeconds i in if t < TimeSpan.FromSeconds 1. then failwith "must be positive" else t)
        let workingDirectory = parseResult.TryPostProcessResult(<@ Working_Directory @>, Path.GetFullPath)

        let sacc = parseResult.PostProcessResult(<@ Storage_Connection_String @>, AzureStorageAccount.FromConnectionString)
        let bacc = parseResult.PostProcessResult(<@ Service_Bus_Connection_string @>, AzureServiceBusAccount.FromConnectionString)

        let config = new Configuration(sacc.ConnectionString, bacc.ConnectionString)
        parseResult.IterResult(<@ Force_Version @>, fun v -> config.Version <- v)
        parseResult.IterResult(<@ Suffix_Id @>, fun id -> config.SuffixId <- id)
        parseResult.IterResult(<@ Use_Version_Suffix @>, fun b -> config.UseVersionSuffix <- b)
        parseResult.IterResult(<@ Use_Suffix_Id @>, fun b -> config.UseSuffixId <- b)
        parseResult.IterResult(<@ Optimize_Closure_Serialization @>, fun o -> config.OptimizeClosureSerialization <- o)

        parseResult.IterResult(<@ Runtime_Queue @>, fun q -> config.WorkItemQueue <- q)
        parseResult.IterResult(<@ Runtime_Topic @>, fun t -> config.WorkItemTopic <- t)

        parseResult.IterResult(<@ Runtime_Container @>, fun c -> config.RuntimeContainer <- c)
        parseResult.IterResult(<@ Assembly_Container @>, fun c -> config.AssemblyContainer <- c)
        parseResult.IterResult(<@ Cloud_Value_Container @>, fun c -> config.CloudValueContainer <- c)

        parseResult.IterResult(<@ Runtime_Table @>, fun c -> config.RuntimeTable <- c)
        parseResult.IterResult(<@ Runtime_Logs_Table @>, fun c -> config.RuntimeLogsTable <- c)
        parseResult.IterResult(<@ User_Data_Table @>, fun c -> config.UserDataTable <- c)

        {
            Configuration = Some config
            MaxWorkItems = maxWorkItems
            WorkerId = workerName
            WorkingDirectory = workingDirectory
            Quiet = quiet
            LogLevel = logLevel
            LogFile = logFile
            HeartbeatInterval = heartbeatInterval
            HeartbeatThreshold = heartbeatThreshold
        }