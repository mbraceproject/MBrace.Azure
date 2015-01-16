namespace MBrace.Azure.Runtime.Common

open System
open System.Runtime.Serialization
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging
open MBrace.Azure.Runtime

open System.Net
open System.Diagnostics
open MBrace
open MBrace.Runtime
open MBrace.Continuation
open System.Collections.Concurrent

type ILogger =
    inherit ICloudLogger
    abstract Attach : ILogger -> unit

type LoggerType =
    | Worker of id : string
    | Client of id : string
    | ProcessLog of id : string
    | Other of name : string * id : string with 
        override this.ToString() = 
            match this with
            | Worker id -> sprintf "worker:%s" id
            | Client id -> sprintf "client:%s" id
            | ProcessLog id -> sprintf "process:%s" id
            | Other(name, id) -> sprintf "%s:%s" name id

type LogRecord(pk, rk, message, time) =
    inherit TableEntity(pk, rk)
    member val Message : string = message with get, set
    member val Time : DateTimeOffset = time with get, set
    new () = new LogRecord(null, null, null, Unchecked.defaultof<_>)

type LoggerBase () =
    let attached = new ConcurrentBag<ILogger>()

    abstract member Log : string -> unit
    override __.Log(msg) = 
        for l in attached do
            l.Log(msg)

    abstract member Attach : ILogger -> unit
    override __.Attach(logger) = attached.Add(logger)

    interface ILogger with
        member __.Attach(entry : ILogger) = __.Attach(entry)
        member __.Log entry = __.Log entry
  

type StorageLogger(config, table : string, loggerType : LoggerType) as this =
    inherit LoggerBase () with
        let maxWaitTime = 5000
        let logs = ResizeArray<LogRecord>()
        let timeToRK (time : DateTimeOffset) = sprintf "%020d" time.Ticks 
        
        let flush () = async {
            let count = logs.Count
            if count > 0 then
                let records =
                    // See *HACK* in log 
                    lock logs (fun () -> 
                                let r = logs.ToArray()
                                logs.Clear()
                                r)
                try
                    do! Table.insertBatch<LogRecord> config table records
                with ex ->
                    this.BaseLog(sprintf "StorageLogger loop failed to write logs : %A" ex)
        }

        do  let rec loop _ = async {
                do! Async.Sleep maxWaitTime
                do! flush ()
                return! loop ()
            }
            Async.Start(loop ())

        let log msg = 
            // HACK: We want to use DateTime.Ticks as a RowKey in table storage,
            // in order to have efficient querying.
            // But RowKeys must be unique per PartitionKey,
            // so we need to make sure that two subsequent calls to __.Log
            // don't produce the same Ticks.
            // We serialize all access to logs by using the lock.
            // Also sleeping for 100 milliseconds to ensure ticks are unique.
            lock logs (fun () ->
                System.Threading.Thread.Sleep(100)
                let time = DateTimeOffset.UtcNow
                let e = new LogRecord(string loggerType, timeToRK time, msg, time)
                logs.Add(e))

        member private __.BaseLog (s) = base.Log(s)

        override __.Log(entry: string) : unit = log entry; base.Log(entry)

        member __.Logf fmt = Printf.ksprintf __.Log fmt

        member __.GetLogs (?loggerType : LoggerType, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) =
            let query = new TableQuery<LogRecord>()
            let filters = 
              [ loggerType |> Option.map (fun pk -> TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, string pk))
                fromDate   |> Option.map (fun t -> TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, timeToRK t))
                toDate     |> Option.map (fun t -> TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, timeToRK t)) ]
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

type NullLogger () =
    inherit LoggerBase () 

type ConsoleLogger () =
    inherit LoggerBase () with
        override x.Log(entry : string) : unit = 
            Console.WriteLine("{0} : {1}", DateTimeOffset.Now.ToString("ddMMyyyy HH:mm:ss.fff zzz"), entry)
            base.Log(entry)

type CustomLogger (f : Action<string>) =
    inherit LoggerBase () with
        override x.Log(entry : string) : unit = 
            f.Invoke(entry)
            base.Log(entry)
 
 // TODO : Remove?       
type ProcessLogger(config, table : string, loggerType : LoggerType) =
    inherit LoggerBase () with
        do match loggerType with
           | ProcessLog _ -> ()
           | _ -> failwith "Invalid logger type %A" loggerType

        let timeToRK (time : DateTimeOffset) = sprintf "%020d" time.Ticks 

        override __.Log(entry : string) : unit = 
            let time = DateTimeOffset.UtcNow
            let e = new LogRecord(string loggerType, timeToRK time, entry, DateTimeOffset.UtcNow)
            Async.RunSync(Table.insert<LogRecord> config table e)
            base.Log(entry)

        member __.GetLogs (?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) =
            let query = new TableQuery<LogRecord>()
            let loggerType = Some loggerType
            let filters = 
              [ loggerType |> Option.map (fun pk -> TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, string pk))
                fromDate   |> Option.map (fun t -> TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, timeToRK t))
                toDate     |> Option.map (fun t -> TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, timeToRK t)) ]
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