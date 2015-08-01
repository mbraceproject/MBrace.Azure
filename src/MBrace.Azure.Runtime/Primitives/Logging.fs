namespace MBrace.Azure.Runtime

open System
open System.Collections.Concurrent
open Microsoft.WindowsAzure.Storage.Table
open MBrace.Core
open MBrace.Core.Internals
open MBrace.Azure
open MBrace.Azure.Runtime.Utilities
open MBrace.Runtime
open System.Runtime.Serialization
open Microsoft.WindowsAzure.Storage

type private LogLevel = MBrace.Runtime.LogLevel

type LoggerType =
    | System of id : string
    | CloudLog of workerId : string * taskId : string
        override this.ToString() = 
            match this with
            | System id -> sprintf "system:%A" id
            | CloudLog(wid, tid) -> sprintf "cloudlog@%A:%A" wid tid

type LogRecord(pk, rk, message, time, level) =
    inherit TableEntity(pk, rk)
    
    member val Level : int = level with get, set
    member val Message : string = message with get, set
    member val Time : DateTimeOffset = time with get, set
    new () = new LogRecord(null, null, null, Unchecked.defaultof<_>, -1)  

type private StorageLoggerMessage =
    | Flush of AsyncReplyChannel<unit>
    | Log of LogRecord

[<Sealed; DataContract>]
type StorageSystemLogger private (storageConn : string, table : string, loggerType : LoggerType) =
    static let timeToRK (time : DateTimeOffset) unique = sprintf "%020d%s" (time.ToUniversalTime().Ticks) unique
    
    let [<DataMember(Name = "storageConn")>] storageConn = storageConn
    let [<DataMember(Name = "table")>] table = table
    let [<DataMember(Name = "config")>] loggerType = loggerType

    let [<IgnoreDataMember>] mutable agent = Unchecked.defaultof<MailboxProcessor<StorageLoggerMessage>>
    let [<IgnoreDataMember>] mutable tableClient = Unchecked.defaultof<CloudTableClient>

    let init () =
        let acc = CloudStorageAccount.Parse(storageConn)
        tableClient <- acc.CreateCloudTableClient()
        let tableRef = tableClient.GetTableReference(table)
        let _ = tableRef.CreateIfNotExists()
        let timespan = TimeSpan.FromSeconds(5.)
        let flush (logs : LogRecord seq) =
            async {
                let tbo = new TableBatchOperation()
                logs |> Seq.iter (tbo.Insert)
                if tbo.Count > 0 then
                    return! Async.AwaitTask(tableRef.ExecuteBatchAsync(tbo))
                            |> Async.Catch
                            |> Async.Ignore
            }
            
        agent <- MailboxProcessor.Start(fun inbox ->
            let rec loop lastWrite (acc : LogRecord list) = async {
                let! msg = inbox.TryReceive(100)
                match msg with
                | None when DateTime.Now - lastWrite >= timespan || acc.Length >= 100 ->
                    do! flush acc
                    return! loop DateTime.Now []
                | Some(Flush(ch)) ->
                    do! flush acc
                    ch.Reply()
                    return! loop DateTime.Now []
                | Some(Log(log)) ->
                    return! loop lastWrite (log :: acc)
                | _ ->
                    return! loop lastWrite acc
            }
            loop DateTime.Now [])

    do init ()

    [<OnDeserialized>]
    let _onDeserialized (_ : StreamingContext) = init ()

    let log msg time level = 
        let e = new LogRecord(string loggerType, timeToRK time (guid()), msg, time, int level)
        agent.Post(Log e)

    interface ISystemLogger with
        member x.LogEntry(level: MBrace.Runtime.LogLevel, time: DateTime, message: string): unit = 
            log message (DateTimeOffset(time)) level

    member __.GetLogs (?loggerType : LoggerType, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) =
        let query = new TableQuery<LogRecord>()
        let lower = Guid.Empty.ToString "N"
        let upper = lower.Replace('0','f')
        let filters = 
            [ loggerType |> Option.map (fun pk -> TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, string pk))
              fromDate   |> Option.map (fun t ->  TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, timeToRK t lower))
              toDate     |> Option.map (fun t ->  TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, timeToRK t upper)) ]
        let filter = 
            filters 
            |> List.fold (fun state filter -> 
                match state, filter with
                | None, None -> None
                | Some f, None 
                | None, Some f -> Some f
                | Some f1, Some f2 -> Some <| TableQuery.CombineFilters(f1, TableOperators.And, f2) ) None
        let query =
            match filter with
            | None -> query
            | Some f -> query.Where(f)

        let acc = CloudStorageAccount.Parse(storageConn)
        tableClient <- acc.CreateCloudTableClient()
        let tableRef = tableClient.GetTableReference(table)
        let _ = tableRef.CreateIfNotExists()
        tableRef.ExecuteQuery(query)


    static member Create(storageConn, table, uuid) = new StorageSystemLogger(storageConn, table, System uuid)
      
type CloudStorageLogger(config : ConfigurationId, workerId : IWorkerId, taskId : string) =
    let loggerType = CloudLog(workerId.Id, taskId)
    let table = config.UserDataTable
    let timeToRK (time : DateTimeOffset) unique = sprintf "%020d%s" (time.ToUniversalTime().Ticks) unique

    interface ICloudLogger with
        override __.Log(entry : string) : unit = 
            let time = DateTimeOffset.UtcNow
            let e = new LogRecord(string loggerType, timeToRK time (guid()), entry, DateTimeOffset.UtcNow, int LogLevel.None)
            Async.RunSync(Table.insert<LogRecord> config table e)

    member __.GetLogs (?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) =
        let query = new TableQuery<LogRecord>()
        let loggerType = Some loggerType
        let lower = Guid.Empty.ToString "N"
        let upper = lower.Replace('0','f')
        let filters = 
            [ loggerType |> Option.map (fun pk -> TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, string pk))
              fromDate   |> Option.map (fun t ->  TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, timeToRK t lower))
              toDate     |> Option.map (fun t ->  TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, timeToRK t upper)) ]
        let filter = 
            filters 
            |> List.fold (fun state filter -> 
                match state, filter with
                | None, None -> None
                | Some f, None 
                | None, Some f -> Some f
                | Some f1, Some f2 -> Some <| TableQuery.CombineFilters(f1, TableOperators.And, f2) ) None
        let query =
            match filter with
            | None -> query
            | Some f -> query.Where(f)
        Table.query config table query


type CustomLogger (f : Action<string>) =
    interface ISystemLogger with
        member x.LogEntry(level: MBrace.Runtime.LogLevel, time: DateTime, message: string): unit = 
            f.Invoke(sprintf "%O %O %O" time level message)

[<AutoOpen>]
module LoggerExtensions =
    type ConsoleLogger with
        member logger.WithColor () =
            { new ISystemLogger with
                member x.LogEntry(level: LogLevel, time: DateTime, message: string): unit = 
                    let current = Console.ForegroundColor
                    Console.ForegroundColor <-
                        match level with
                        | LogLevel.Error -> ConsoleColor.Red
                        | LogLevel.Warning -> ConsoleColor.Yellow
                        | LogLevel.Info -> ConsoleColor.Cyan
                        | LogLevel.Debug -> ConsoleColor.White
                        | _ -> current
                    (logger :> ISystemLogger).LogEntry(level, time, message)
                    Console.ForegroundColor <- current
            }
    
    type AttacheableLogger with
        static member FromLoggers(loggers : ISystemLogger seq) = 
            let logger = new AttacheableLogger()
            for l in loggers do
                ignore(logger.AttachLogger(l))
            logger

    type ISystemLogger with
        member this.LogInfof fmt = Printf.ksprintf (fun s -> this.LogInfo s) fmt
        member this.LogErrorf fmt = Printf.ksprintf (fun s -> this.LogError s) fmt
        member this.LogWarningf fmt = Printf.ksprintf (fun s -> this.LogWarning s) fmt
