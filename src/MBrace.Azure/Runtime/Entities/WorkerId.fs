namespace MBrace.Azure.Runtime

open System

open Microsoft.WindowsAzure.Storage.Table

open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.PerformanceMonitor
open MBrace.Azure.Runtime.Utilities

[<AutoSerializable(true)>]
type WorkerId internal (workerId : string) = 
    member this.Id = workerId

    interface IWorkerId with
        member this.CompareTo(obj: obj): int =
            match obj with
            | :? WorkerId as w -> compare workerId w.Id
            | _ -> invalidArg "obj" "invalid comparand."
        
        member this.Id: string = this.Id

    override this.ToString() = this.Id
    override this.Equals(other:obj) =
        match other with
        | :? WorkerId as w -> workerId = w.Id
        | _ -> false

    override this.GetHashCode() = hash workerId

[<AllowNullLiteral>]
type WorkerRecord(workerId : string) =
    inherit TableEntity(WorkerRecord.DefaultPartitionKey, workerId)
    
    member val Id                   = workerId with get, set
    member val Hostname             = Unchecked.defaultof<string> with get, set
    member val ProcessId            = Nullable<int>() with get, set
    member val ProcessName          = Unchecked.defaultof<string> with get, set
    member val InitializationTime   = Nullable<DateTimeOffset>() with get, set
    member val LastHeartbeat        = Nullable<DateTimeOffset>() with get, set
    member val MaxWorkItems         = Nullable<int>()   with get, set
    member val ActiveWorkItems      = Nullable<int>()   with get, set
    member val ProcessorCount       = Nullable<int>()   with get, set
    member val MaxClockSpeed        = Nullable<double>() with get, set
    member val CPU                  = Nullable<double>() with get, set
    member val TotalMemory          = Nullable<double>() with get, set
    member val Memory               = Nullable<double>() with get, set
    member val NetworkUp            = Nullable<double>() with get, set
    member val NetworkDown          = Nullable<double>() with get, set
    member val HeartbeatInterval    = Nullable<int64>() with get, set
    member val HeartbeatThreshold   = Nullable<int64>() with get, set
    member val Platform             = Unchecked.defaultof<string> with get, set
    member val Runtime              = Unchecked.defaultof<string> with get, set
    member val Version              = Unchecked.defaultof<string> with get, set
    member val Status               = Unchecked.defaultof<string> with get, set
    
    new () = new WorkerRecord(null)

    member this.GetCounters () : PerformanceInfo =
        { 
            CpuUsage = this.CPU
            MaxClockSpeed = this.MaxClockSpeed 
            TotalMemory = this.TotalMemory
            MemoryUsage = this.Memory
            NetworkUsageUp = this.NetworkUp
            NetworkUsageDown = this.NetworkDown
            Platform = match this.Platform with null -> Nullable() | pf -> Nullable<_>(Enum.Parse(typeof<Platform>, pf) :?> Platform)
            Runtime = match this.Runtime with null -> Nullable() | pf -> Nullable<_>(Enum.Parse(typeof<Runtime>, pf) :?> Runtime)
        }

    member this.UpdateCounters(counters : PerformanceInfo) =
            this.CPU <- counters.CpuUsage
            this.TotalMemory <- counters.TotalMemory
            this.Memory <- counters.MemoryUsage
            this.NetworkUp <- counters.NetworkUsageUp
            this.NetworkDown <- counters.NetworkUsageDown
            this.MaxClockSpeed <- counters.MaxClockSpeed
            this.LastHeartbeat <- nullable DateTimeOffset.Now
            this.Runtime <- if counters.Runtime.HasValue then counters.Runtime.Value.ToString() else null
            this.Platform <- if counters.Platform.HasValue then counters.Platform.Value.ToString() else null

    member this.CloneDefault() =
        let p = new WorkerRecord()
        p.PartitionKey <- this.PartitionKey
        p.RowKey <- this.RowKey
        p.ETag <- this.ETag
        p

    override this.ToString () = sprintf "worker:%A" this.Id

    static member DefaultPartitionKey = "worker"