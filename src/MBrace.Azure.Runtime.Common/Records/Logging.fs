namespace MBrace.Azure.Runtime.Common

open System
open Microsoft.WindowsAzure.Storage.Table

open MBrace.Continuation
open System.Collections.Concurrent
open MBrace.Runtime

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

type LoggerCombiner (loggers) =
    let attached = new ConcurrentBag<ICloudLogger>(loggers)

    member __.Attach(logger) = attached.Add(logger)

    interface ICloudLogger with
        member __.Log entry =
            for l in attached do
                l.Log(entry)

    new () = LoggerCombiner(Seq.empty)
    
  

type StorageLogger(config, table : string, loggerType : LoggerType) =
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
            do! Table.insertBatch<LogRecord> config table records
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

    interface ICloudLogger with
        override __.Log(entry: string) : unit = log entry


    member __.GetLogs (?loggerType : LoggerType, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) =
        let query = new TableQuery<LogRecord>()
        let filters = 
            [ loggerType |> Option.map (fun pk -> TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, string pk))
              fromDate   |> Option.map (fun t ->  TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, timeToRK t))
              toDate     |> Option.map (fun t ->  TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, timeToRK t)) ]
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
    interface ICloudLogger with
        member __.Log(_: string): unit = ()
        
type ConsoleLogger () =
    let format = "ddMMyyyy HH:mm:ss.fff zzz" // 31012015 15:42:50.404 +02:00
    let prettyPrint (message : string) =
        let offset = format.Length + 4
        let space = new String(' ', offset)
        let sb = new System.Text.StringBuilder()
        let mutable first = true
        for line in message.Split('\n') do
            if first then first <- false else sb.Append(space) |> ignore
            sb.AppendLine(line) |> ignore
        sb.ToString()

    interface ICloudLogger with
        override __.Log(entry : string) : unit = 
            Console.Write("{0} {1}", DateTimeOffset.Now.ToString(format), prettyPrint entry)

type CustomLogger (f : Action<string>) =
    interface ICloudLogger with
        override x.Log(entry : string) : unit = 
            f.Invoke(entry)
 
 // TODO : Remove?       
type ProcessLogger(config, table : string, pid : string) =
    let loggerType = ProcessLog pid

    let timeToRK (time : DateTimeOffset) = sprintf "%020d" time.Ticks 

    interface ICloudLogger with
        override __.Log(entry : string) : unit = 
            let time = DateTimeOffset.UtcNow
            let e = new LogRecord(string loggerType, timeToRK time, entry, DateTimeOffset.UtcNow)
            Async.RunSync(Table.insert<LogRecord> config table e)
            Threading.Thread.Sleep(100) // See HACK

    member __.GetLogs (?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) =
        let query = new TableQuery<LogRecord>()
        let loggerType = Some loggerType
        let filters = 
            [ loggerType |> Option.map (fun pk -> TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, string pk))
              fromDate   |> Option.map (fun t ->  TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, timeToRK t))
              toDate     |> Option.map (fun t ->  TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, timeToRK t)) ]
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