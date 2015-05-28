namespace MBrace.Azure

open System
open MBrace.Core
open MBrace.Core.Internals

/// Represents current worker status.
type WorkerStatus = 
    /// Worker initialization.
    | Initializing
    /// Worker successfully started.
    | Running
    /// Worker stopped.
    | Stopped
    /// Unexpected worker exception.
    | Faulted of exn

    override this.ToString() =
        match this with
        | Initializing  -> "Initializing"
        | Running   -> "Running"
        | Stopped   -> "Stopped"
        | Faulted _ -> "Faulted"

    static member Pickle (status : WorkerStatus) = MBrace.Azure.Runtime.Configuration.Pickler.Pickle(status)

    static member UnPickle (bytes) =
        if bytes = null then Unchecked.defaultof<_> else MBrace.Azure.Runtime.Configuration.Pickler.UnPickle<WorkerStatus>(bytes)


namespace MBrace.Azure.Runtime.Info

open System
open System.Net
open System.Threading

open Microsoft.WindowsAzure.Storage.Table
open Microsoft.FSharp.Linq.NullableOperators

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Azure
open MBrace.Runtime.Utils
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities


type WorkerRecord(pk, id, hostname : string, pid : Nullable<int>, pname : string, joined : DateTimeOffset) =
    inherit TableEntity(pk, id)
    
    member val Hostname           = hostname          with get, set
    member val Id                 = id                with get, set
    member val ProcessId          = pid               with get, set
    member val ProcessName        = pname             with get, set
    member val InitializationTime = joined            with get, set
    member val ConfigurationId    = Unchecked.defaultof<byte []> with get, set
    member val MaxJobs            = Nullable<int>()   with get, set
    member val ActiveJobs         = Nullable<int>()   with get, set
    member val ProcessorCount     = Environment.ProcessorCount with get, set
    member val CPU                = Nullable<double>() with get, set
    member val TotalMemory        = Nullable<double>() with get, set
    member val Memory             = Nullable<double>() with get, set
    member val NetworkUp          = Nullable<double>() with get, set
    member val NetworkDown        = Nullable<double>() with get, set
    member val Version            = Unchecked.defaultof<string> with get, set
    member val Status             = Unchecked.defaultof<byte []> with get, set
    new () = new WorkerRecord(null, null, null, nullableDefault, null, Unchecked.defaultof<_>)

    member this.UpdateCounters(counters : NodePerformanceInfo) =
            this.CPU <- counters.CpuUsage
            this.TotalMemory <- counters.TotalMemory
            this.Memory <- counters.MemoryUsage
            this.NetworkUp <- counters.NetworkUsageUp
            this.NetworkDown <- counters.NetworkUsageDown

    member this.CloneDefault() =
        let p = new WorkerRecord()
        p.PartitionKey <- this.PartitionKey
        p.RowKey <- this.RowKey
        p.ETag <- this.ETag
        p

type private WorkerManagerMessage =
    | Start       of WorkerRecord * TimeSpan
    | Stop        of AsyncReplyChannel<unit>
    | SetFaulted  of Exception
    | SetRunning  of AsyncReplyChannel<unit>
    | SetJobCount of jobCount : int
    
type private WorkerManagerState =
    {
        Worker             : WorkerRecord
        LastHeartBeatFault : bool
        InitialTimeSpan    : TimeSpan
        CurrentTimeSpan    : TimeSpan
    }


namespace MBrace.Azure

open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Info
open MBrace.Azure.Runtime.Utilities
open System
open MBrace.Core
open System.Runtime.Serialization

/// IWorkerRef implementation for MBrace.Azure workers.
[<Sealed; Serializable>]
type WorkerRef internal (config : ConfigurationId, partitionKey, rowKey) =
    let mutable enableUpdates = false
    let mutable record = Table.read config config.RuntimeTable partitionKey rowKey
                         |> Async.RunSynchronously
    let mutable live = Unchecked.defaultof<Live<WorkerRecord>>
    let getLive() = 
        Live<WorkerRecord>(
            (fun () -> Table.read config config.RuntimeTable partitionKey rowKey),
            Choice1Of2(record),
            keepLast = false,
            interval = 1000,
            stopf = fun _ -> not enableUpdates)
      
    let getRecord() =
        if enableUpdates then 
            record <- live.Value
        record
    
    /// Enable automatic updates of WorkerRef properties. Defaults to false.        
    member this.AutoUpdate
        with get () = enableUpdates
        and  set e = 
            enableUpdates <- e
            if not enableUpdates then live.Stop()
            else live <- getLive()

    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, _ : StreamingContext): unit = 
            info.AddValue("config", config, typeof<ConfigurationId>)
            info.AddValue("partitionKey", partitionKey, typeof<string>)
            info.AddValue("rowKey", rowKey, typeof<string>)
        
    new (info: SerializationInfo, _ : StreamingContext) =
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        let partitionKey = info.GetValue("partitionKey", typeof<string>) :?> string
        let rowKey = info.GetValue("rowKey", typeof<string>) :?> string
        new WorkerRef(config, partitionKey, rowKey)

    /// Worker/Service Id.
    member this.Id : string = getRecord().Id
    
    ///Current worker status.
    member this.Status : WorkerStatus = WorkerStatus.UnPickle(getRecord().Status)
    
    /// Machine's name.
    member this.Hostname : string = getRecord().Hostname
    
    /// Host process id.
    member this.ProcessId : int = getRecord().ProcessId.GetValueOrDefault(-1)
    
    /// Host process name.
    member this.ProcessName : string = getRecord().ProcessName
    
    /// First worker's heartbeat time.
    member this.InitializationTime : DateTimeOffset = getRecord().InitializationTime
    
    /// Last worker's heartbeat time.
    member this.HeartbeatTime : DateTimeOffset = getRecord().Timestamp
    
    /// Worker activated ConfigurationId.
    member this.ConfigurationId : ConfigurationId = config
    
    /// Worker's MaxConcurrentJobCount.
    member this.MaxJobCount : int = getRecord().MaxJobs.GetValueOrDefault(-1)
    
    /// Workers' processor count.
    member this.ProcessorCount : int = getRecord().ProcessorCount
    
    /// Current dequeued jobs.
    member this.ActiveJobs : int = getRecord().ActiveJobs.GetValueOrDefault(-1)
    
    /// CPU usage.
    member this.CPU : double = getRecord().CPU.GetValueOrDefault(-1.)
    
    /// Total memory (MB).
    member this.TotalMemory : double = getRecord().TotalMemory.GetValueOrDefault(-1.)
    
    /// Memory used (MB).
    member this.Memory : double = getRecord().Memory.GetValueOrDefault(-1.)
    
    /// Network upload (KB/s).
    member this.NetworkUp : double = getRecord().NetworkUp.GetValueOrDefault(-1.)
    
    /// Network download (KB/s).
    member this.NetworkDown : double = getRecord().NetworkDown.GetValueOrDefault(-1.)
    
    /// Runtime Version
    member this.Version : string = getRecord().Version
    
    override this.GetHashCode() = hash this.Id
    override this.Equals(other:obj) =
        match other with
        | :? WorkerRef as w -> this.Id = w.Id
        | _ -> false

    interface IComparable with
        member this.CompareTo(obj: obj): int = 
            match obj with
            | :? WorkerRef as y -> compare this.Id ((y :> IWorkerRef).Id) 
            | _ -> invalidArg "obj" "Invalid IWorkerRef instance."
        
    interface IWorkerRef with
        member this.Hostname: string = this.Hostname
        member this.Id = this.Id
        member this.Type = "MBrace.Azure.Worker"
        member this.ProcessorCount = this.ProcessorCount

namespace MBrace.Azure.Runtime.Info

open System
open System.Net
open MBrace.Azure
open MBrace.Runtime.Utils
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities
open MBrace.Core.Internals

[<AutoSerializable(false)>]
type WorkerManager private (config : ConfigurationId, logger : ICloudLogger) =
    static let pk = "WorkerRef"
    
    let table = config.RuntimeTable

    let perfMon = lazy new PerformanceMonitor()
    let mutable current = None : WorkerRecord option
    let runningPickle = WorkerStatus.Pickle Running

    let heartbeatAgent = 
        let maxHeartbeatInterval = TimeSpan.FromSeconds(32.)

        new MailboxProcessor<WorkerManagerMessage>(fun inbox ->
            let rec loop (state : WorkerManagerState option) = async {
                let! message = async {
                    if inbox.CurrentQueueLength > 0 then 
                        let! msg = inbox.Receive()
                        return Some msg
                    else return None
                }
                
                match message, state with
                | None, None ->
                    do! Async.Sleep 100
                    return! loop state
                | None, (Some { Worker = worker; LastHeartBeatFault = fault; InitialTimeSpan = initialTs; CurrentTimeSpan = currentTs } as state) ->
                    let! fault', currentTs' = async {
                        try
                            let counters = perfMon.Value.GetCounters()
                            worker.UpdateCounters(counters)
                            worker.ETag <- "*"
                            let! e = Table.merge<WorkerRecord> config table worker
                            current <- Some e
                            if fault then
                                let newTs = max (TimeSpan.FromTicks(currentTs.Ticks / 2L)) initialTs
                                logger.Logf "Decreasing timespan to %A" newTs
                                return false, newTs
                            elif currentTs > initialTs then
                                let newTs = max (TimeSpan.FromTicks(currentTs.Ticks / 2L)) initialTs
                                logger.Logf "Decreasing timespan to %A" newTs
                                return true, newTs
                            else
                                return false, currentTs
                        with ex ->
                            logger.Logf "Failed to give heartbeat %A" ex
                            let newTimeSpan = min (TimeSpan.FromTicks(currentTs.Ticks * 2L)) maxHeartbeatInterval
                            logger.Logf "Increasing timespan to %A" newTimeSpan
                            return true, newTimeSpan
                        }
                    do! Async.Sleep (int currentTs'.TotalMilliseconds)
                        
                    if fault' = fault && currentTs' = currentTs then
                        return! loop state
                    else
                        return! loop (Some { Worker = worker; LastHeartBeatFault = fault'; InitialTimeSpan = initialTs; CurrentTimeSpan = currentTs' })
                | Some(Start(worker, ts)), None ->
                    logger.Logf "Starting heartbeat loop with interval %A" ts
                    worker.Status <- runningPickle
                    return! loop (Some { Worker = worker; LastHeartBeatFault = false; InitialTimeSpan = ts; CurrentTimeSpan = ts })
                | Some(SetJobCount(jc)), Some state ->
                    try
                        state.Worker.ActiveJobs <- nullable jc
                        let! c = Table.replace config table state.Worker
                        current <- Some c
                    with ex -> logger.Logf "Heartbeat Loop Table update error: %A" ex
                    return! loop (Some state)
                | Some(SetRunning(ch)), Some state ->
                    try
                        state.Worker.Status <- runningPickle
                        let! c = Table.replace config table state.Worker
                        current <- Some c
                    with ex -> logger.Logf "Heartbeat Loop Table update error: %A" ex
                    ch.Reply()
                    return! loop (Some {state with LastHeartBeatFault = false })
                | Some(SetFaulted(ex)), Some state ->
                    try
                        state.Worker.Status <- WorkerStatus.Pickle (Faulted ex)
                        let! c = Table.replace config table state.Worker
                        current <- Some c
                    with ex -> logger.Logf "Heartbeat Loop Table update error: %A" ex
                    return! loop (Some state)
                | Some(Stop(ch)), Some state ->
                    try
                        state.Worker.Status <- WorkerStatus.Pickle WorkerStatus.Stopped
                        let! c = Table.replace config table state.Worker
                        current <- Some c
                    with ex -> logger.Logf "Heartbeat Loop Table update error: %A" ex
                    ch.Reply()
                    return! loop None
                | other ->
                    logger.Logf "Heartbeat Loop : Invalid state : %A" other
                    failwithf "Invalid state %A" other
            }
            loop None
        )

    do heartbeatAgent.Error.Add(fun ex -> logger.Logf "Heartbeat Loop error %A" ex)

    member this.Current : WorkerRecord = 
        match current with
        | Some c -> c
        | None -> failwith "No worker registered."
    
    member this.SetJobCountLocal(jc) = heartbeatAgent.Post(SetJobCount jc)

    member this.SetCurrentAsRunning() = heartbeatAgent.PostAndAsyncReply(SetRunning)

    member this.SetCurrentAsFaulted(ex : Exception) =
        heartbeatAgent.Post(SetFaulted(ex))

    member this.StartHeartbeatLoop(timespan : TimeSpan) =
        if timespan > WorkerManager.MaxHeartbeatTimeSpan then raise(ArgumentOutOfRangeException("timespan", "Max TimeSpan of 10 minutes allowed."))
        heartbeatAgent.Start()
        heartbeatAgent.Post(Start(this.Current, timespan))

    member this.RegisterLocal(workerId) =
        async {
            let! record = Table.read<WorkerRecord> config table pk workerId
            current <- Some record
        }

    member this.RegisterCurrent(workerId : string, ?maxJobs) : Async<unit> = 
        async {
            match current with 
            | Some w -> 
                return failwithf "Worker %A is active" w
            | None ->
                perfMon.Value.Start()
                let ps = Diagnostics.Process.GetCurrentProcess()
                let joined = DateTimeOffset.UtcNow
                let w = new WorkerRecord(pk, workerId, Dns.GetHostName(), nullable ps.Id, ps.ProcessName, joined)
                w.UpdateCounters(perfMon.Value.GetCounters())
                w.ActiveJobs <- nullable 0
                w.Status <-  WorkerStatus.Pickle Initializing
                w.Version <- ReleaseInfo.localVersion.ToString(4)
                w.MaxJobs <- match maxJobs with None -> nullableDefault | Some mj -> nullable mj
                w.ConfigurationId <- Configuration.Pickler.Pickle config
                do! Table.insertOrReplace<WorkerRecord> config table w //Worker might restart but keep id.
                current <- Some w
        }

    member this.SetCurrentAsStopped () : Async<unit> = 
        async {
            (perfMon.Value :> IDisposable).Dispose() 
            return! heartbeatAgent.PostAndAsyncReply(fun ch -> Stop(ch))
        }

    member this.SetWorkerStopped(worker : WorkerRecord) : Async<unit> =
        async {
            worker.Status <- WorkerStatus.Pickle Stopped
            let! _ = Table.replace config table worker
            return ()
        }

    member this.DeleteWorkerRecord(workerId : string) : Async<unit> =
        async {
            let! record = this.GetWorker(workerId)
            do! Table.delete config table record
        }

    member this.GetWorker(workerId : string) = 
        async {
            return! Table.read<WorkerRecord> config table pk workerId
        }

    member this.GetWorkers(timespan : TimeSpan, showStarting : bool, showInactive : bool, showFaulted : bool) : Async<WorkerRecord seq> = async {
        let! ws = Table.queryPK<WorkerRecord> config table pk
        // TODO : Make timespan part of the query?
        return ws |> Seq.filter (fun w -> DateTimeOffset.UtcNow - w.Timestamp < timespan)
                  |> Seq.filter (fun w ->
                        match Configuration.Pickler.UnPickle<WorkerStatus> w.Status with
                        | Running -> true
                        | Initializing -> showStarting
                        | Stopped -> showInactive
                        | Faulted _ -> showFaulted )
    }

    member this.GetWorkerRefs(timespan : TimeSpan, showStarting : bool, showInactive : bool, showFaulted : bool) : Async<WorkerRef seq> =
        async {
            let! wr = this.GetWorkers(timespan, showStarting, showInactive, showFaulted)
            return wr |> Seq.map (fun w -> new WorkerRef(Configuration.Pickler.UnPickle(w.ConfigurationId) ,w.PartitionKey, w.RowKey))
        }

    /// Max timespan used to determine if a worker is alive.
    static member MaxHeartbeatTimeSpan = TimeSpan.FromMinutes(10.) // http://blogs.msdn.com/b/kwill/archive/2011/05/05/windows-azure-role-architecture.aspx
    
    static member Create(config : ConfigurationId, logger : ICloudLogger) = new WorkerManager(config, logger)

    /// Unrecoverable worker faults.
    static member SetFaulted (config : ConfigurationId, workerId : string, ex : exn) : Async<unit> =
        async {
            let ps = Diagnostics.Process.GetCurrentProcess()
            let joined = DateTimeOffset.UtcNow
            let w = new WorkerRecord(pk, workerId, Dns.GetHostName(), nullable ps.Id, ps.ProcessName, joined)
            w.Status <- WorkerStatus.Pickle (Faulted ex)
            w.ETag <- "*"
            do! Table.insertOrReplace config config.RuntimeTable w
                |> Async.Ignore
        }