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
open MBrace.Runtime.Utils.PrettyPrinters

type private LogLevel = MBrace.Runtime.LogLevel

module private Logger =
    let mkSystemLogPartitionKey (loggerId : string) = sprintf "systemlog:%s" loggerId
    let mkCloudLogPartitionKey  (taskId : string) = sprintf "cloudlog:%s" taskId
    let timeToRowKey (time : DateTimeOffset) unique = sprintf "%020d%s" (time.ToUniversalTime().Ticks) unique


type SystemLogRecord(pk, rk, message, time, level, loggerId, isSysLog) =
    inherit TableEntity(pk, rk)
    
    member val Level : int = level with get, set 
    member val Message : string = message with get, set
    member val Time : DateTimeOffset = time with get, set
    member val LoggerId : string = loggerId with get, set
    new () = new SystemLogRecord(null, null, null, Unchecked.defaultof<_>, -1, null, true)  

type CloudLogRecord(pk, rk, message, time, workerId, taskId, jobId) =
    inherit TableEntity(pk, rk)
    
    member val Message : string = message with get, set
    member val Time : DateTimeOffset = time with get, set
    member val WorkerId : string = workerId with get, set
    member val TaskId : string = taskId with get, set
    member val JobId : Guid = jobId with get, set
    new () = new CloudLogRecord(null, null, null, Unchecked.defaultof<_>, null, null, Unchecked.defaultof<_>)  


type internal LogReporter() = 
    static let template : Field<SystemLogRecord> list = 
        [ Field.create "Source" Left (fun p -> p.LoggerId)
          Field.create "Message" Left (fun p -> p.Message)
          Field.create "Level" Left (fun p -> enum<LogLevel> p.Level)
          Field.create "Timestamp" Right (fun p -> let pt = p.Time in pt.ToString("ddMMyyyy HH:mm:ss.fff zzz")) ]
    
    static member Report(logs : SystemLogRecord seq) = 
        let ls = logs 
                 |> Seq.sortBy (fun l -> l.Time, l.PartitionKey)
                 |> Seq.toList
        Record.PrettyPrint(template, ls, "Logs", false)

type private StorageLoggerMessage =
    | Flush of AsyncReplyChannel<unit>
    | Log of SystemLogRecord

[<Sealed; DataContract>]
type SystemLogger private (storageConn : string, table : string, loggerId : string) =
    static let timeToRK (time : DateTimeOffset) unique = sprintf "%020d%s" (time.ToUniversalTime().Ticks) unique
    
    let [<DataMember(Name = "storageConn")>] storageConn = storageConn
    let [<DataMember(Name = "table")>] table = table
    let [<DataMember(Name = "loggerId")>] loggerId = loggerId

    let [<IgnoreDataMember>] mutable agent = Unchecked.defaultof<MailboxProcessor<StorageLoggerMessage>>
    let [<IgnoreDataMember>] mutable tableClient = Unchecked.defaultof<CloudTableClient>

    let init () =
        let acc = CloudStorageAccount.Parse(storageConn)
        tableClient <- acc.CreateCloudTableClient()
        let tableRef = tableClient.GetTableReference(table)
        let _ = tableRef.CreateIfNotExists()
        let timespan = TimeSpan.FromSeconds(5.)
        let flush (logs : SystemLogRecord seq) =
            async {
                let tbo = new TableBatchOperation()
                logs |> Seq.iter (tbo.Insert)
                if tbo.Count > 0 then
                    return! Async.AwaitTask(tableRef.ExecuteBatchAsync(tbo))
                            |> Async.Catch
                            |> Async.Ignore
            }
            
        agent <- MailboxProcessor.Start(fun inbox ->
            let rec loop lastWrite (acc : SystemLogRecord list) = async {
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
        let e = new SystemLogRecord(Logger.mkSystemLogPartitionKey loggerId, timeToRK time (guid()), msg, time, int level, loggerId, true)
        agent.Post(Log e)

    interface ISystemLogger with
        member x.LogEntry(entry : SystemLogEntry) =
            log entry.Message (DateTimeOffset(entry.DateTime)) entry.LogLevel

    member this.ShowLogs(?loggerId : string, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) =
        this.GetLogs(?loggerId = loggerId, ?fromDate = fromDate, ?toDate = toDate)
        |> LogReporter.Report
        |> Console.WriteLine

    member __.GetLogs(?loggerId : string, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) =
        let query = new TableQuery<SystemLogRecord>()
        let lower = Guid.Empty.ToString "N"
        let upper = lower.Replace('0','f')
        let filters = 
            [ loggerId  |> Option.map (fun pk -> TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, Logger.mkSystemLogPartitionKey pk))
              fromDate  |> Option.map (fun t ->  TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, timeToRK t lower))
              toDate    |> Option.map (fun t ->  TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, timeToRK t upper)) ]
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


    static member Create(storageConn, table, uuid) = new SystemLogger(storageConn, table, uuid)
      
/// CloudLogger implementation over Table storage.
type CloudLogWriter (config : ConfigurationId, workerId : IWorkerId, taskId : string, jobId : CloudJobId) =
    interface ICloudJobLogger with
        member this.Dispose(): unit = ()

    interface ICloudLogger with
        override __.Log(entry : string) : unit = 
            let time = DateTimeOffset.UtcNow
            let e = new CloudLogRecord(Logger.mkCloudLogPartitionKey taskId, Logger.timeToRowKey time (guid()), entry, time, workerId.Id, taskId, jobId)
            Async.RunSync(Table.insert<CloudLogRecord> config config.UserDataTable e)

/// Fetch cloudlogs
[<Sealed; AutoSerializable(false)>]
type CloudLogReader (config : ConfigurationId, taskId : string) =

    member this.GetObservable () : Async<IObservable<CloudLogEntry>> =
        async {
            let syncRoot = new obj()
            let observers = new ResizeArray<IObserver<CloudLogEntry>>()
            let rec pollLoop (lastDate : DateTimeOffset option) = async {
                let! logs = Async.Catch <| this.GetLogs(?fromDate = lastDate)
                let date =
                    match logs with
                    | Choice1Of2 logs when Seq.isEmpty logs ->
                        lastDate
                    | Choice1Of2 logs ->
                        let log = logs |> Seq.maxBy (fun l -> l.DateTime)
                        observers |> Seq.iter (fun o -> logs |> Seq.iter o.OnNext)
                        Some <| DateTimeOffset(log.DateTime)
                    | Choice2Of2 ex ->
                        observers |> Seq.iter (fun o -> o.OnError ex)
                        lastDate
                do! Async.Sleep 500
                return! pollLoop date

            }

            let! _ = Async.StartChild(pollLoop None)

            let observable = 
                { new IObservable<CloudLogEntry> with
                      member this.Subscribe(observer: IObserver<CloudLogEntry>): IDisposable = 
                          lock syncRoot (fun _ -> observers.Add(observer))
                          { new IDisposable with
                            member this.Dispose () =
                                lock syncRoot (fun _ -> 
                                    if not <| observers.Remove(observer) then
                                        raise <| ObjectDisposedException(string observer) ) }
                       }
            return observable
        }

    member this.GetLogs (?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) : Async<CloudLogEntry seq> =
        async {
            let query = new TableQuery<CloudLogRecord>()
            let lower = Guid.Empty.ToString "N"
            let upper = lower.Replace('0','f')
            let filters = 
                [ Some(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, Logger.mkCloudLogPartitionKey taskId))
                  fromDate   |> Option.map (fun t ->  TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, Logger.timeToRowKey t lower))
                  toDate     |> Option.map (fun t ->  TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, Logger.timeToRowKey t upper)) ]
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
            let! logs = Table.query config config.UserDataTable query
            return logs
                   |> Seq.map (fun log ->
                        CloudLogEntry(log.TaskId, log.WorkerId, log.JobId, log.Time.Date, log.Message)
                   )
        }

[<Sealed; AutoSerializable(false)>]
type CloudLogManager private (config : ConfigurationId) =
    interface ICloudLogManager with
        member this.CreateJobLogger(worker: IWorkerId, job: CloudJob): Async<ICloudJobLogger> = 
            async {
                return new CloudLogWriter(config, worker, job.TaskEntry.Id, job.Id) :> _
            }
        
        member this.GetAllCloudLogsByTask(taskId: string): Async<seq<CloudLogEntry>> = 
            async {
                return! CloudLogReader(config, taskId).GetLogs()
            }
        
        member this.GetCloudLogObservableByTask(taskId: string): Async<IObservable<CloudLogEntry>> = 
            async {
                return! CloudLogReader(config, taskId).GetObservable()
            }

    static member Create(config : ConfigurationId) = new CloudLogManager(config)
        

type CustomLogger (f : Action<string>) =
    interface ISystemLogger with
        member x.LogEntry(entry : SystemLogEntry): unit = 
            f.Invoke(sprintf "%O %O %O" entry.DateTime entry.LogLevel entry.Message)

[<AutoOpen>]
module LoggerExtensions =
    
    type AttacheableLogger with
        static member FromLoggers(loggers : ISystemLogger seq) = 
            let logger = AttacheableLogger.Create(makeAsynchronous = false)
            for l in loggers do
                ignore(logger.AttachLogger(l))
            logger

    type ISystemLogger with
        member this.LogInfof fmt = Printf.ksprintf (fun s -> this.LogInfo s) fmt
        member this.LogErrorf fmt = Printf.ksprintf (fun s -> this.LogError s) fmt
        member this.LogWarningf fmt = Printf.ksprintf (fun s -> this.LogWarning s) fmt
