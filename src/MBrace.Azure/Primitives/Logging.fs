namespace MBrace.Azure.Runtime

open System
open System.Collections.Concurrent
open System.Threading
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
    let timeToRowKey (time : DateTimeOffset) unique = sprintf "%020d%s" (time.UtcDateTime.Ticks) unique


type SystemLogRecord(pk, rk, message, time, level, loggerId) =
    inherit TableEntity(pk, rk)
    
    member val Level : int = level with get, set 
    member val Message : string = message with get, set
    member val Time : DateTimeOffset = time with get, set
    member val LoggerId : string = loggerId with get, set

    new () = new SystemLogRecord(null, null, null, Unchecked.defaultof<_>, -1, null)

    member slr.ToLogEntry() =
        new SystemLogEntry(enum slr.Level, slr.Message, slr.Time, slr.LoggerId)

type CloudLogRecord(pk, rk, message, time, workerId, taskId, jobId) =
    inherit TableEntity(pk, rk)
    
    member val Message : string = message with get, set
    member val Time : DateTimeOffset = time with get, set
    member val WorkerId : string = workerId with get, set
    member val TaskId : string = taskId with get, set
    member val WorkItemId : Guid = jobId with get, set

    new () = new CloudLogRecord(null, null, null, Unchecked.defaultof<_>, null, null, Unchecked.defaultof<_>)

    member clr.ToLogEntry() =
        new CloudLogEntry(clr.TaskId, clr.WorkerId, clr.WorkItemId, clr.Time, clr.Message)

type private StorageLoggerMessage =
    | Flush of AsyncReplyChannel<unit>
    | Log of SystemLogRecord

// TODO: refactor
[<Sealed; DataContract>]
type TableStorageSystemLogger private (storageConn : string, table : string, loggerId : string) =
    let [<DataMember(Name = "storageConn")>] storageConn = Validate.storageConn storageConn
    let [<DataMember(Name = "table")>] table = Validate.tableName table
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
        let e = new SystemLogRecord(Logger.mkSystemLogPartitionKey loggerId, Logger.timeToRowKey time (guid()), msg, time, int level, loggerId)
        agent.Post(Log e)

    interface ISystemLogger with
        member x.LogEntry(entry : SystemLogEntry) =
            log entry.Message entry.DateTime entry.LogLevel

    member __.GetLogs(?loggerId : string, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) : Async<SystemLogEntry []> = async {
        let query = new TableQuery<SystemLogRecord>()
        let lower = Guid.Empty.ToString "N"
        let upper = lower.Replace('0','f')
        let filters = 
            [ loggerId  |> Option.map (fun pk -> TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, Logger.mkSystemLogPartitionKey pk))
              fromDate  |> Option.map (fun t ->  TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, Logger.timeToRowKey t lower))
              toDate    |> Option.map (fun t ->  TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, Logger.timeToRowKey t upper)) ]
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
        let! _ = tableRef.CreateIfNotExistsAsync()
        // TODO : make asynchronous
        let result = tableRef.ExecuteQuery(query)
        return result |> Seq.map (fun r -> r.ToLogEntry()) |> Seq.toArray
    }

    member this.GetSystemLogPoller (?loggerId : string) : ILogPoller<SystemLogEntry> =
        let event = new Event<SystemLogEntry> ()
        let rec pollLoop (lastDate : DateTimeOffset option) = async {
            let! logs = this.GetLogs(?fromDate = lastDate, ?loggerId = loggerId) |> Async.Catch

            let! date = async {
                match logs with
                | Choice2Of2 _ -> return lastDate
                | Choice1Of2 logs when Seq.isEmpty logs -> return lastDate
                | Choice1Of2 logs ->
                    let lastLog = logs |> Array.maxBy (fun l -> l.DateTime)
                    let! _ = Async.StartChild(async {do for l in logs do event.Trigger l})
                    let dto = lastLog.DateTime.AddTicks(1L)
                    return Some dto
            }

            do! Async.Sleep 500
            return! pollLoop date
        }

        let cts = new CancellationTokenSource()
        let _ = Async.StartAsTask(pollLoop None, cancellationToken = cts.Token)

        { new ILogPoller<SystemLogEntry> with
            member x.AddHandler(handler: Handler<SystemLogEntry>): unit = 
                event.Publish.AddHandler handler
              
            member x.Dispose(): unit = cts.Cancel()
              
            member x.RemoveHandler(handler: Handler<SystemLogEntry>): unit = 
                event.Publish.RemoveHandler handler
              
            member x.Subscribe(observer: IObserver<SystemLogEntry>): IDisposable = 
                event.Publish.Subscribe observer
        }

    static member Create(storageConn, table, uuid) = new TableStorageSystemLogger(storageConn, table, uuid)
      
/// CloudLogger implementation over Table storage.
type CloudLogWriter (config : ConfigurationId, workerId : IWorkerId, taskId : string, jobId : CloudWorkItemId) =
    interface ICloudWorkItemLogger with
        member this.Dispose(): unit = ()

    interface ICloudLogger with
        override __.Log(entry : string) : unit = 
            let time = DateTimeOffset.UtcNow
            let e = new CloudLogRecord(Logger.mkCloudLogPartitionKey taskId, Logger.timeToRowKey time (guid()), entry, time, workerId.Id, taskId, jobId)
            Async.RunSync(Table.insert<CloudLogRecord> config config.UserDataTable e)

/// Fetch cloudlogs
[<Sealed; AutoSerializable(false)>]
type CloudLogReader (config : ConfigurationId, taskId : string) =

    member this.GetLogs (?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) : Async<CloudLogEntry []> =
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
            return logs |> Array.map (fun l -> l.ToLogEntry())
        }

    member this.GetLogPoller () : ILogPoller<CloudLogEntry> =
        let event = new Event<CloudLogEntry>()
        let rec pollLoop (lastDate : DateTimeOffset option) = async {
            let! logs = Async.Catch <| this.GetLogs(?fromDate = lastDate)
            let! date = async {
                match logs with
                | Choice2Of2 _ -> return lastDate
                | Choice1Of2 logs when Array.isEmpty logs -> return lastDate
                | Choice1Of2 logs ->
                    let lastLog = logs |> Array.maxBy (fun l -> l.DateTime)
                    let! _ = Async.StartChild(async { do for l in logs do event.Trigger l })
                    let dto = lastLog.DateTime.AddTicks(1L)
                    return Some dto
            }

            do! Async.Sleep 500
            return! pollLoop date
        }

        let cts = new CancellationTokenSource()
        let _ = Async.StartAsTask(pollLoop None, cancellationToken = cts.Token)

        { new ILogPoller<CloudLogEntry> with
            member x.AddHandler(handler: Handler<CloudLogEntry>): unit = 
                event.Publish.AddHandler handler
                      
            member x.Dispose(): unit = cts.Cancel()
                      
            member x.RemoveHandler(handler: Handler<CloudLogEntry>): unit = 
                event.Publish.RemoveHandler handler
                      
            member x.Subscribe(observer: IObserver<CloudLogEntry>): IDisposable = 
                event.Publish.Subscribe observer }

[<Sealed; AutoSerializable(false)>]
type CloudLogManager (config : ConfigurationId) =
    interface ICloudLogManager with
        member this.CreateWorkItemLogger(worker: IWorkerId, workItem: CloudWorkItem): Async<ICloudWorkItemLogger> = async {
            return new CloudLogWriter(config, worker, workItem.TaskEntry.Id, workItem.Id) :> _
        }
        
        member this.GetAllCloudLogsByTask(taskId: string): Async<seq<CloudLogEntry>> = async {
            let! logs = CloudLogReader(config, taskId).GetLogs()
            return logs :> seq<_>
        }
        
        member this.GetCloudLogPollerByTask(taskId: string): Async<ILogPoller<CloudLogEntry>> = async {
            return CloudLogReader(config, taskId).GetLogPoller()
        }

[<Sealed; AutoSerializable(false)>]
type SystemLogManager (config : Configuration) =
    let mkTableLogger (loggerId : string) =
        TableStorageSystemLogger.Create(config.StorageConnectionString, config.GetConfigurationId().RuntimeLogsTable, loggerId)

    let defaultLogger = lazy(mkTableLogger (mkUUID())) // dummy assignment; provisional.

    interface IRuntimeSystemLogManager with        
        member x.GetRuntimeLogs(): Async<seq<SystemLogEntry>> = async {
            let! logs = defaultLogger.Value.GetLogs()
            return logs :> seq<_>
        }
        
        member x.GetWorkerLogs(id: IWorkerId): Async<seq<SystemLogEntry>> = async {
            let! logs = defaultLogger.Value.GetLogs(loggerId = id.Id)
            return logs :> seq<_>
        }

        member x.ClearLogs(): Async<unit> = async {
            return raise <| NotImplementedException()
        }
        
        member x.ClearLogs(_: IWorkerId): Async<unit> = async {
            return raise <| NotImplementedException()
        }
        
        member x.CreateLogPoller(): Async<ILogPoller<SystemLogEntry>> = async {
            return defaultLogger.Value.GetSystemLogPoller()
        }
        
        member x.CreateLogWriter(id: IWorkerId): Async<ISystemLogger> = async {
//            return mkTableLogger id.Id :> _
            return raise <| NotImplementedException()
        }
        
        member x.CreateWorkerLogPoller(id: IWorkerId): Async<ILogPoller<SystemLogEntry>> = async {
            return defaultLogger.Value.GetSystemLogPoller(loggerId = id.Id)
        }

type CustomLogger (f : Action<string>) =
    interface ISystemLogger with
        member x.LogEntry(entry : SystemLogEntry): unit = 
            f.Invoke(sprintf "%O %O %O" entry.DateTime entry.LogLevel entry.Message)

[<AutoOpen>]
module LoggerExtensions =
    type ISystemLogger with
        member this.LogInfof fmt = Printf.ksprintf (fun s -> this.LogInfo s) fmt
        member this.LogErrorf fmt = Printf.ksprintf (fun s -> this.LogError s) fmt
        member this.LogWarningf fmt = Printf.ksprintf (fun s -> this.LogWarning s) fmt
