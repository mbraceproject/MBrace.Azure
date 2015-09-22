namespace MBrace.Azure.Runtime

open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.Runtime.Serialization
open System.Threading

open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.PrettyPrinters
open MBrace.Azure
open MBrace.Azure.Runtime.Utilities

module private Logger =
    let mkSystemLogPartitionKey (loggerId : string) = sprintf "systemlog:%s" loggerId
    let mkCloudLogPartitionKey  (taskId : string) = sprintf "cloudlog:%s" taskId
    let timeToRowKey (time : DateTimeOffset) unique = sprintf "%020d-%s" time.UtcDateTime.Ticks unique
    let timeToRowKeyPrefix (time : DateTimeOffset) = sprintf "%020d" time.UtcDateTime.Ticks

[<AutoOpen>]
module LoggerExtensions =
    type ISystemLogger with
        member this.LogInfof fmt = Printf.ksprintf (fun s -> this.LogInfo s) fmt
        member this.LogErrorf fmt = Printf.ksprintf (fun s -> this.LogError s) fmt
        member this.LogWarningf fmt = Printf.ksprintf (fun s -> this.LogWarning s) fmt

/// System log record that inherits Azure's TableEntity type
type SystemLogRecord(partitionKey : string, rowKey : string, message : string, time : DateTimeOffset, level : int, loggerId : string) =
    inherit TableEntity(partitionKey, rowKey)
    
    member val Level    = level with get, set 
    member val Message  = message with get, set
    member val Time     = time with get, set
    member val LoggerId = loggerId with get, set

    new () = new SystemLogRecord(null, null, null, Unchecked.defaultof<_>, -1, null)

    /// Converts LogEntry table entity to MBrace.Runtime.SystemLogEntry struct
    member slr.ToLogEntry() =
        new SystemLogEntry(enum slr.Level, slr.Message, slr.Time, slr.LoggerId)

    /// <summary>
    ///     Creates a table system log record using provided info and MBrace.Runtime.SystemLogEntry 
    /// </summary>
    /// <param name="partitionKey">Table partition key.</param>
    /// <param name="rowKey">Table row key.</param>
    /// <param name="loggerId">Logger source identifier.</param>
    /// <param name="entry">Input log entry.</param>
    static member FromLogEntry(partitionKey : string, rowKey : string, loggerId : string, entry : SystemLogEntry) =
        new SystemLogRecord(partitionKey, rowKey, entry.Message, entry.DateTime, int entry.LogLevel, loggerId)

/// Cloud process log record that inherits Azure's TableEntity type
type CloudLogRecord(partitionKey : string, rowKey : string, message : string, time : DateTimeOffset, workerId : string, procId : string, workItemId : Guid) =
    inherit TableEntity(partitionKey, rowKey)
    
    member val Message      = message with get, set
    member val Time         = time with get, set
    member val WorkerId     = workerId with get, set
    member val ProcessId    = procId with get, set
    member val WorkItemId   = workItemId with get, set

    new () = new CloudLogRecord(null, null, null, Unchecked.defaultof<_>, null, null, Unchecked.defaultof<_>)

    /// Converts LogEntry table entity to MBrace.Runtime.SystemLogEntry struct
    member clr.ToLogEntry() =
        new CloudLogEntry(clr.ProcessId, clr.WorkerId, clr.WorkItemId, clr.Time, clr.Message)

    /// <summary>
    ///     Creates a table system log record using provided info and MBrace.Runtime.CloudLogEntry 
    /// </summary>
    /// <param name="partitionKey">Table partition key.</param>
    /// <param name="rowKey">Table row key.</param>
    /// <param name="entry">Input log entry.</param>
    static member FromLogEntry(partitionKey : string, rowKey : string, entry : CloudLogEntry) =
        new CloudLogRecord(partitionKey, rowKey, entry.Message, entry.DateTime, entry.WorkerId, entry.CloudProcessId, entry.WorkItem)


[<AutoSerializable(false)>]
type private TableLoggerMessage<'Entry when 'Entry :> TableEntity> =
    | Flush of AsyncReplyChannel<unit>
    | Log of 'Entry

/// Local agent that writes batches of log entries to table store
[<AutoSerializable(false)>]
type private CloudTableLogWriter<'Entry when 'Entry :> TableEntity> private (table : CloudTable, timespan : TimeSpan, logThreshold : int) =

    let queue = new Queue<'Entry> ()
    let flush () = async {
        if queue.Count > 0 then
            let tbo = new TableBatchOperation()
            for log in queue do tbo.Insert log
            do!
                table.ExecuteBatchAsync tbo
                |> Async.AwaitTask
                |> Async.Catch
                |> Async.Ignore

            queue.Clear()
    }

    let rec loop (lastWrite : DateTime) (inbox : MailboxProcessor<TableLoggerMessage<'Entry>>) = async {
        let! msg = inbox.TryReceive(100)
        match msg with
        | None when DateTime.Now - lastWrite >= timespan || queue.Count >= logThreshold ->
            do! flush ()
            return! loop DateTime.Now inbox
        | Some(Flush(ch)) ->
            do! flush ()
            ch.Reply()
            return! loop DateTime.Now inbox
        | Some(Log(log)) ->
            queue.Enqueue log
            return! loop lastWrite inbox
        | _ ->
            return! loop lastWrite inbox
    }

    let cts = new CancellationTokenSource()
    let agent = MailboxProcessor.Start(loop DateTime.Now, cancellationToken = cts.Token)

    /// Appends a new entry to the write queue.
    member __.LogEntry(entry : 'Entry) = agent.Post (Log entry)

    interface IDisposable with
        member __.Dispose () = 
            agent.PostAndReply Flush
            cts.Cancel ()

    /// <summary>
    ///     Creates a local log writer instance with supplied table, timespan, and log threshold parameters.
    /// </summary>
    /// <param name="table">Cloud table to persist logs.</param>
    /// <param name="timespan">Timespan after which any log should be persisted.</param>
    /// <param name="logThreshold">Minimum number of logs to force instance flushing of log entries.</param>
    static member Create(table : CloudTable, ?timespan : TimeSpan, ?logThreshold : int) = async {
        let timespan = defaultArg timespan (TimeSpan.FromSeconds 5.)
        let logThreshold = defaultArg logThreshold 100
        do! table.CreateIfNotExistsAsync()   
        return new CloudTableLogWriter<'Entry>(table, timespan, logThreshold)
    }

/// Defines a local polling agent for subscribing table log events
[<AutoSerializable(false)>]
type CloudTableLogPoller<'Entry> private (fetch : DateTimeOffset option -> Async<seq<'Entry>>, getDate : 'Entry -> DateTimeOffset, interval : TimeSpan) =
    let event = new Event<'Entry> ()
    let rec pollLoop (filtered : HashSet<'Entry>) (lastDate : DateTimeOffset option) = async {
        do! Async.Sleep (int interval.TotalMilliseconds)
        let! logs = fetch lastDate |> Async.Catch

        match logs with
        | Choice2Of2 _ -> 
            do! Async.Sleep 1000
            return! pollLoop filtered lastDate

        | Choice1Of2 logs ->
            let logs = logs |> Seq.sortBy getDate |> Seq.filter (filtered.Contains >> not) |> Seq.toArray
            if Array.isEmpty logs then 
                return! pollLoop filtered lastDate
            else
                do for l in logs do try event.Trigger l with _ -> ()
                let lastDate = Array.last logs |> getDate
                let filtered = logs |> Seq.filter (fun l -> getDate l = lastDate) |> hset
                return! pollLoop filtered (Some lastDate)
    }

    let cts = new CancellationTokenSource()
    let _ = Async.StartAsTask(pollLoop (new HashSet<_>()) None, cancellationToken = cts.Token)

    [<CLIEvent>]
    member __.Publish = event.Publish

    interface IDisposable with
        member __.Dispose() = cts.Cancel()

    static member Create(fetch : DateTimeOffset option -> Async<seq<'Entry>>, getDate : 'Entry -> DateTimeOffset, ?interval) =
        let interval = defaultArg interval (TimeSpan.FromMilliseconds 500.)
        new CloudTableLogPoller<'Entry>(fetch, getDate, interval)


/// Management object for table storage based log files
[<AutoSerializable(false)>]
type TableSystemLogManager (config : Configuration) =
    let account = CloudStorageAccount.Parse config.StorageConnectionString
    let tableClient = account.CreateCloudTableClient()
    let table = tableClient.GetTableReference(config.RuntimeLogsTable)

    /// <summary>
    ///     Creates a local log writer using provided logger id.
    /// </summary>
    /// <param name="loggerId">Logger identifier.</param>
    member __.CreateLogWriter(loggerId : string) = async {
        let! writer = CloudTableLogWriter<SystemLogRecord>.Create(table)
        return {
            new obj ()

                interface ISystemLogger with
                    member __.LogEntry(e : SystemLogEntry) =
                        let record = SystemLogRecord.FromLogEntry(Logger.mkSystemLogPartitionKey loggerId, Logger.timeToRowKey e.DateTime (guid()), loggerId, e)
                        writer.LogEntry record

                interface IDisposable with
                    member __.Dispose() =
                        Disposable.dispose writer
        }
    }
        
    /// <summary>
    ///     Fetches logs matching specified constraints from table storage.
    /// </summary>
    /// <param name="loggerId">Constrain to specific logger identifier.</param>
    /// <param name="fromDate">Log entries start date.</param>
    /// <param name="toDate">Log entries finish date.</param>
    member __.GetLogs(?loggerId : string, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) : Async<seq<SystemLogEntry>> = async {
        let query = new TableQuery<SystemLogRecord>()
        let filters = 
            [ loggerId  |> Option.map (fun pk -> TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, Logger.mkSystemLogPartitionKey pk))
              fromDate  |> Option.map (fun t ->  TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, Logger.timeToRowKeyPrefix t))
              toDate    |> Option.map (fun t ->  TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, Logger.timeToRowKeyPrefix (t.AddTicks 1L))) ]

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

        do! table.CreateIfNotExistsAsync()
        // TODO : make asynchronous
        let result = table.ExecuteQuery query
        return result |> Seq.map (fun r -> r.ToLogEntry())
    }

    /// <summary>
    ///     Gets a log entry observable that asynchronously polls for new logs.
    /// </summary>
    /// <param name="loggerId">Generating logger id constraint.</param>
    member this.GetSystemLogPoller (?loggerId : string) : ILogPoller<SystemLogEntry> =
        let getLogs lastDate = this.GetLogs(?loggerId = loggerId, ?fromDate = lastDate)
        let getDate (e : SystemLogEntry) = e.DateTime
        let poller = CloudTableLogPoller<SystemLogEntry>.Create(getLogs, getDate)

        { new ILogPoller<SystemLogEntry> with
            member x.AddHandler(handler: Handler<SystemLogEntry>): unit = 
                poller.Publish.AddHandler handler
              
            member x.Dispose(): unit = Disposable.dispose poller
              
            member x.RemoveHandler(handler: Handler<SystemLogEntry>): unit = 
                poller.Publish.RemoveHandler handler
              
            member x.Subscribe(observer: IObserver<SystemLogEntry>): IDisposable = 
                poller.Publish.Subscribe observer
        }

    interface IRuntimeSystemLogManager with 
        member x.CreateLogWriter(id: IWorkerId): Async<ISystemLogger> = async {
            return! x.CreateLogWriter(id.Id)
        }
               
        member x.GetRuntimeLogs(): Async<seq<SystemLogEntry>> = async {
            let! logs = x.GetLogs()
            return logs |> Seq.sortBy (fun e -> e.DateTime)
        }
        
        member x.GetWorkerLogs(id: IWorkerId): Async<seq<SystemLogEntry>> = async {
            let! logs = x.GetLogs(loggerId = id.Id)
            return logs |> Seq.sortBy (fun e -> e.DateTime)
        }
        
        member x.CreateLogPoller(): Async<ILogPoller<SystemLogEntry>> = async {
            return x.GetSystemLogPoller()
        }
        
        member x.CreateWorkerLogPoller(id: IWorkerId): Async<ILogPoller<SystemLogEntry>> = async {
            return x.GetSystemLogPoller(loggerId = id.Id)
        }

        member x.ClearLogs(): Async<unit> = async {
            return raise <| NotImplementedException()
        }
        
        member x.ClearLogs(_: IWorkerId): Async<unit> = async {
            return raise <| NotImplementedException()
        }

/// Management object for writing cloud process logs to the table store
[<AutoSerializable(false)>]
type TableCloudLogManager (config : Configuration) =
    let account = CloudStorageAccount.Parse config.StorageConnectionString
    let tableClient = account.CreateCloudTableClient()
    let table = tableClient.GetTableReference config.UserDataTable

    /// <summary>
    ///     Fetches all cloud process log entries satisfying given constraints.
    /// </summary>
    /// <param name="processId">Cloud process identifier.</param>
    /// <param name="fromDate">Start date constraint.</param>
    /// <param name="toDate">Stop date constraint.</param>
    member this.GetLogs (processId : string, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) : Async<seq<CloudLogEntry>> =
        async {
            let query = new TableQuery<CloudLogRecord>()
            let filters = 
                [ Some(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, Logger.mkCloudLogPartitionKey processId))
                  fromDate   |> Option.map (fun t -> TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, Logger.timeToRowKeyPrefix t))
                  toDate     |> Option.map (fun t -> TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, Logger.timeToRowKeyPrefix (t.AddTicks 1L))) ]

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

            do! table.CreateIfNotExistsAsync()
            let logs = table.ExecuteQuery query
            return logs |> Seq.map (fun l -> l.ToLogEntry())
        }

    /// <summary>
    ///     Fetches a cloud process log entry observable that asynchonously polls the store for new log entries.
    /// </summary>
    /// <param name="processId">Process identifier.</param>
    member this.GetLogPoller (processId : string) : ILogPoller<CloudLogEntry> =
        let getLogs lastDate = this.GetLogs(processId, ?fromDate = lastDate)
        let getDate (e : CloudLogEntry) = e.DateTime
        let poller = CloudTableLogPoller<CloudLogEntry>.Create(getLogs, getDate)

        { new ILogPoller<CloudLogEntry> with
            member x.AddHandler(handler: Handler<CloudLogEntry>): unit = 
                poller.Publish.AddHandler handler
                      
            member x.Dispose(): unit = Disposable.dispose poller
                      
            member x.RemoveHandler(handler: Handler<CloudLogEntry>): unit = 
                poller.Publish.RemoveHandler handler
                      
            member x.Subscribe(observer: IObserver<CloudLogEntry>): IDisposable = 
                poller.Publish.Subscribe observer }

    interface ICloudLogManager with
        member this.CreateWorkItemLogger(worker: IWorkerId, workItem: CloudWorkItem): Async<ICloudWorkItemLogger> = async {
            let! writer = CloudTableLogWriter<CloudLogRecord>.Create(table)
            let partitionKey = Logger.mkCloudLogPartitionKey workItem.Process.Id
            return {
                new ICloudWorkItemLogger with
                    member __.Log(message : string) =
                        let time = DateTimeOffset.Now
                        let entry = CloudLogEntry(workItem.Process.Id, worker.Id, workItem.Id, time, message)
                        let record = CloudLogRecord.FromLogEntry(partitionKey, Logger.timeToRowKey time (guid()), entry)
                        writer.LogEntry record

                    member __.Dispose() =
                        Disposable.dispose writer
            }
        }
        
        member this.GetAllCloudLogsByProcess(taskId: string): Async<seq<CloudLogEntry>> = async {
            let! logs = this.GetLogs(taskId)
            return logs |> Seq.sortBy (fun e -> e.DateTime)
        }
        
        member this.GetCloudLogPollerByProcess(taskId: string): Async<ILogPoller<CloudLogEntry>> = async {
            return this.GetLogPoller(taskId)
        }