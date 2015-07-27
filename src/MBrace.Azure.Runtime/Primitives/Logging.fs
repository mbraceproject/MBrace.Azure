namespace MBrace.Azure.Runtime

open System
open System.Collections.Concurrent
open Microsoft.WindowsAzure.Storage.Table
open MBrace.Core
open MBrace.Core.Internals
open MBrace.Azure
open MBrace.Azure.Runtime.Utilities
open MBrace.Runtime

type LoggerType =
    | System of id : string
    | CloudLog of taskId : string
        override this.ToString() = 
            match this with
            | System id -> sprintf "client:%s" id
            | CloudLog id -> sprintf "cloudlog:%s" id

type LogRecord(pk, rk, message, time, level) =
    inherit TableEntity(pk, rk)
    
    member val Level : int = level with get, set
    member val Message : string = message with get, set
    member val Time : DateTimeOffset = time with get, set
    new () = new LogRecord(null, null, null, Unchecked.defaultof<_>, -1)  

type StorageSystemLogger private (config : ConfigurationId, loggerType : LoggerType) =
    let maxWaitTime = 5000
    let table = config.RuntimeLogsTable
    let logs = ResizeArray<LogRecord>()
    let timeToRK (time : DateTimeOffset) unique = sprintf "%020d%s" (time.ToUniversalTime().Ticks) unique
    let mutable running = true

    let flush () = async {
        let count = logs.Count
        if count > 0 then
            let records =
                lock logs (fun () -> 
                            let r = logs.ToArray()
                            logs.Clear()
                            r)
            let! result = Async.Catch <| Table.insertBatch<LogRecord> config table records
            match result with
            | Choice1Of2 _ -> ()
            | Choice2Of2 ex ->
                printf "Failed to log %A" ex
                lock logs (fun () -> logs.AddRange(records))
    }

    do  let rec loop _ = async {
            do! Async.Sleep maxWaitTime
            if running then
                do! flush ()
            return! loop ()
        }
        Async.Start(loop ())

    let log msg time level = 
        lock logs (fun () ->
            let e = new LogRecord(string loggerType, timeToRK time (guid()), msg, time, int level)
            logs.Add(e))

    interface ISystemLogger with
        member x.LogEntry(level: LogLevel, time: DateTime, message: string): unit = 
            log message (DateTimeOffset(time)) level

    member __.Start () = running <- true
    member __.Stop () = 
        Async.RunSync(flush())
        running <- false

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
        Table.query config table query


    static member Create(config, uuid) = new StorageSystemLogger(config, System uuid)
      
type CloudStorageLogger(config : ConfigurationId, taskId : string) =
    let loggerType = CloudLog taskId
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
    interface ICloudLogger with
        override x.Log(entry : string) : unit = 
            f.Invoke(entry)

[<AutoOpen>]
module LoggerExtensions =
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
