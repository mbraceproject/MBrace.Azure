module MBrace.Azure.Runtime.Arguments

open Nessos.Argu

open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Runtime

type private AzureArguments =
    // General-purpose arguments
    | Log_Level of int
    | Max_Work_Items of int
    | Worker_Name of string
    // Connection string parameters
    | [<Mandatory>][<AltCommandLine("-s")>] Storage_Connection_String of string
    | [<Mandatory>][<AltCommandLine("-b")>] Service_Bus_Connection_string of string
    // Cluster configuration parameters
    | Force_Version of string
    | Suffix_Id of uint16
    | Use_Version_Suffix of bool
    | Use_Suffix_Id of bool
    // ServiceBus
    | Runtime_Queue of string
    | Runtime_Topic of string
    // Blob Storage
    | Runtime_Container of string
    | User_Data_Container of string
    | Assembly_Container of string
    | Cloud_Value_Container of string
    // Table Storage
    | Runtime_Table of string
    | Runtime_Logs_Table of string
    | User_Data_Table of string
with
    interface IArgParserTemplate with
        member arg.Usage =
            match arg with
            | Log_Level _ -> "Log level for worker system logs. Defaults to Info."
            | Max_Work_Items _ -> "Specify maximum number of concurrent work items."
            | Worker_Name _ -> "Specify worker name identifier."
            | Storage_Connection_String _ -> "Azure Storage connection string."
            | Service_Bus_Connection_string _ -> "Azure ServiceBus connection string."
            | Force_Version _ -> "Forces an MBrace.Azure version number identifier. Defaults to compiled version."
            | Suffix_Id _ -> "User-supplied suffix identifier for Azure store resources. Defaults to 0."
            | Use_Version_Suffix _ -> "Enables or disables version suffix in store resources. Defaults to true."
            | Use_Suffix_Id _ -> "Enables or disables user-supplied suffix identifier in store resources. Defaults to true."
            | Runtime_Queue _ -> "Specifies the work item queue name in the ServiceBus."
            | Runtime_Topic _ -> "Specifies the work item topic name in the ServiceBus."
            | Runtime_Container _ -> "Specifies the blob container name used for persisting MBrace cluster data."
            | User_Data_Container _ -> "Specifies the blob container name used for persisting MBrace user data."
            | Assembly_Container _ -> "Specifies the blob container name used for persisting Assembly dependencies."
            | Cloud_Value_Container _ -> "Specifies the blob container name used for persisting CloudValue dependencies."
            | Runtime_Table _ -> "Specifies the table name used for writing MBrace cluster entries."
            | Runtime_Logs_Table _ -> "Specifies the table name used for writing MBrace cluster system log entries."
            | User_Data_Table _ -> "Specifies the table name used for writing user logs."


let private argParser = ArgumentParser.Create<AzureArguments>()

/// Configuration object encoding command line parameters for an MBrace.Azure process
type ArgumentConfiguration = 
    {
        Configuration : Configuration
        MaxWorkItems : int option
        WorkerName : string option
        LogLevel : LogLevel option
    }
with
    /// Creates a configuration object using supplied parameters.
    static member Create(config : Configuration, ?maxWorkItems, ?workerName, ?logLevel) =
        { Configuration = config ; MaxWorkItems = maxWorkItems ; WorkerName = workerName ; LogLevel = logLevel }

    /// Converts a configuration object to a command line string.
    static member ToCommandLineArguments(cfg : ArgumentConfiguration) =
        let args = [

            match cfg.MaxWorkItems with Some w -> yield Max_Work_Items w | None -> ()
            match cfg.WorkerName with Some n -> yield Worker_Name n | None -> ()
            match cfg.LogLevel with Some l -> yield Log_Level (int l) | None -> ()

            let config = cfg.Configuration

            yield Storage_Connection_String config.StorageConnectionString
            yield Service_Bus_Connection_string config.ServiceBusConnectionString

            yield Force_Version config.Version
            yield Suffix_Id config.SuffixId
            yield Use_Version_Suffix config.UseVersionSuffix
            yield Use_Suffix_Id config.UseSuffixId

            yield Runtime_Queue config.RuntimeQueue
            yield Runtime_Topic config.RuntimeTopic

            yield Runtime_Container config.RuntimeContainer
            yield User_Data_Container config.UserDataContainer
            yield Assembly_Container config.AssemblyContainer
            yield Cloud_Value_Container config.CloudValueContainer

            yield Runtime_Table config.RuntimeTable
            yield Runtime_Logs_Table config.RuntimeLogsTable
            yield User_Data_Table config.UserDataTable
        ]

        argParser.PrintCommandLineFlat args

    /// Parses command line arguments to a configuration object using Argu.
    static member FromCommandLineArguments(args : string []) =
        let parseResult = argParser.Parse(args, errorHandler = new ProcessExiter())

        let maxWorkItems = parseResult.TryGetResult <@ Max_Work_Items @>
        let logLevel = parseResult.TryPostProcessResult(<@ Log_Level @>, enum<LogLevel>)
        let workerName = parseResult.TryGetResult <@ Worker_Name @>

        let sconn = parseResult.GetResult <@ Storage_Connection_String @>
        let bconn = parseResult.GetResult <@ Service_Bus_Connection_string @>

        let config = new Configuration(sconn, bconn)
        parseResult.IterResult(<@ Force_Version @>, fun v -> config.Version <- v)
        parseResult.IterResult(<@ Suffix_Id @>, fun id -> config.SuffixId <- id)
        parseResult.IterResult(<@ Use_Version_Suffix @>, fun b -> config.UseVersionSuffix <- b)
        parseResult.IterResult(<@ Use_Suffix_Id @>, fun b -> config.UseVersionSuffix <- b)

        parseResult.IterResult(<@ Runtime_Queue @>, fun q -> config.RuntimeQueue <- q)
        parseResult.IterResult(<@ Runtime_Topic @>, fun t -> config.RuntimeTopic <- t)

        parseResult.IterResult(<@ Runtime_Container @>, fun c -> config.RuntimeContainer <- c)
        parseResult.IterResult(<@ User_Data_Container @>, fun c -> config.UserDataContainer <- c)
        parseResult.IterResult(<@ Assembly_Container @>, fun c -> config.AssemblyContainer <- c)
        parseResult.IterResult(<@ Cloud_Value_Container @>, fun c -> config.CloudValueContainer <- c)

        parseResult.IterResult(<@ Runtime_Table @>, fun c -> config.RuntimeTable <- c)
        parseResult.IterResult(<@ Runtime_Logs_Table @>, fun c -> config.RuntimeLogsTable <- c)
        parseResult.IterResult(<@ User_Data_Table @>, fun c -> config.UserDataTable <- c)

        {
            Configuration = config
            MaxWorkItems = maxWorkItems
            WorkerName = workerName
            LogLevel = logLevel
        }