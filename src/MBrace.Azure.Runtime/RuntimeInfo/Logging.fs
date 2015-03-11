namespace MBrace.Azure.Runtime.Info

open System
open Microsoft.WindowsAzure.Storage.Table
open MBrace.Azure
open MBrace.Continuation
open System.Collections.Concurrent
open MBrace.Runtime
open MBrace.Azure.Runtime.Utilities

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

type LoggerCombiner (loggers, showAppDomain) =
    let attached = new ConcurrentBag<ICloudLogger>(loggers)
    let appDomain = AppDomain.CurrentDomain.FriendlyName

    member __.Attach(logger) = attached.Add(logger)
    member val ShowAppDomainAsPrefix = showAppDomain with get, set

    interface ICloudLogger with
        member this.Log entry =
            let entry = if this.ShowAppDomainAsPrefix then sprintf "AppDomain %s\n%s" appDomain entry else entry
            for l in attached do
                l.Log(entry)

    new () = LoggerCombiner(Seq.empty, false)
    new (showAppDomain) = LoggerCombiner(Seq.empty, showAppDomain)
    
  

type StorageLogger(config : ConfigurationId, loggerType : LoggerType) =
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

    let log msg = 
        lock logs (fun () ->
            let time = DateTimeOffset.UtcNow
            let e = new LogRecord(string loggerType, timeToRK time (guid()), msg, time)
            logs.Add(e))

    interface ICloudLogger with
        override __.Log(entry: string) : unit = log entry

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

 
 // TODO : Remove?       
type ProcessLogger(config : ConfigurationId, pid : string) =
    let loggerType = ProcessLog pid
    let table = config.UserDataTable
    let timeToRK (time : DateTimeOffset) unique = sprintf "%020d%s" (time.ToUniversalTime().Ticks) unique

    interface ICloudLogger with
        override __.Log(entry : string) : unit = 
            let time = DateTimeOffset.UtcNow
            let e = new LogRecord(string loggerType, timeToRK time (guid()), entry, DateTimeOffset.UtcNow)
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

namespace MBrace.Azure

open System
open MBrace.Runtime

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

type TimeEllapsedLogger (logger : ICloudLogger) =
    let sync = new Object()
    let time = ref DateTime.Now
    let touch () = lock sync (fun () -> time := DateTime.Now)

    interface ICloudLogger with
        member x.Log(entry: string): unit =
            let elapsed = DateTime.Now - time.Value
            let msg = sprintf "[Elapsed %A] %s" elapsed entry
            logger.Log(msg)
            touch ()   

type CustomLogger (f : Action<string>) =
    interface ICloudLogger with
        override x.Log(entry : string) : unit = 
            f.Invoke(entry)