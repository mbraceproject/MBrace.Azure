namespace MBrace.Azure.Client

open MBrace
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Info
open MBrace.Azure.Runtime.Utilities
open MBrace.Azure.Runtime.Primitives
open MBrace.Runtime
open MBrace.Continuation
open MBrace.Runtime.Compiler
open MBrace.Runtime.Utils.PrettyPrinters
open System
open System.IO
open System.Threading
open System.Reflection
open MBrace.Azure
open System.Collections.Concurrent


[<AutoSerializable(false); AbstractClass>]
/// Represents a cloud process.
type Process internal (config, pid : string, ty : Type, pmon : ProcessManager) = 
    
    let proc = 
        new Live<_>((fun () -> pmon.GetProcess(pid)), initial = Choice2Of2(exn ("Process not initialized")), 
                    keepLast = true, interval = 500, 
                    stopf = 
                        function 
                        | Choice1Of2 p when p.Completed -> true
                        | _ -> false )

    let logger = new ProcessLogger(config, pid)
    let dcts = lazy DistributedCancellationTokenSource.FromPath(config, proc.Value.Id, proc.Value.CancellationUri)

    member internal this.ProcessEntity = proc
    member internal this.DistributedCancellationTokenSource = dcts.Value
    
    /// Block until process record is updated. 
    /// Workaround because ResultCell.SetResult and Process.SetCompleted
    /// are not batched.
    member internal this.AwaitCompletionAsync () = 
        let rec loop () = async {
            if proc.Value.Completed then 
                return () 
            else 
                do! Async.Sleep 100 
                return! loop ()
        }
        loop ()

    /// Awaits for the process result.
    abstract AwaitResultBoxed : unit -> obj
    /// Asynchronously waits for the process result.
    abstract AwaitResultBoxedAsync : unit -> Async<obj>

    /// Returns process' CancellationTokenSource.
    member this.CancellationTokenSource = dcts.Value :> ICloudCancellationTokenSource

    /// Process id.    
    member this.Id = pid

    /// Process name.
    member this.Name = proc.Value.Name

    /// Process type.
    member this.Type = ty

    /// Returns the initialization time for this process.
    member this.InitializationTime = let init = proc.Value.InitializationTime in init.ToLocalTime()
    
    /// Returns the execution time for this process.
    member this.ExecutionTime = 
        let s = 
            if proc.Value.Completed then proc.Value.CompletionTime
            else DateTimeOffset.UtcNow
        s - proc.Value.InitializationTime
    
    /// Returns iff the process is completed.
    member this.Completed = proc.Value.Completed

    /// Returns the number of tasks created by this process and are currently executing.
    member this.ActiveJobs = proc.Value.ActiveJobs

    /// Returns the number of tasks created by this process.
    member this.TotalJobs = proc.Value.TotalJobs

    /// Returns the number of tasks completed by this process.
    member this.CompletedJobs = proc.Value.CompletedJobs

    /// Returns the number of tasks failed to execute by this process.
    member this.FaultedJobs = proc.Value.FaultedJobs

    /// Sends a kill signal for this process.
    member this.Kill() = Async.RunSync(this.KillAsync())
    /// Asynchronously sends a kill signal for this process.
    member this.KillAsync() = async {
            do! pmon.SetKillRequested(pid)
            do this.DistributedCancellationTokenSource.Cancel()
        }

    /// Asynchronously returns all cloud logs for this process.
    member this.GetLogsAsync(?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) = 
        logger.GetLogs(?fromDate = fromDate, ?toDate = toDate)
    /// Returns all cloud logs for this process.
    member this.GetLogs(?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) =
        Async.RunSync(this.GetLogsAsync(?fromDate = fromDate, ?toDate = toDate))

    /// Prints all cloud logs for this process.
    member this.ShowLogs(?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) = 
        printf "%s" <| LogReporter.Report(this.GetLogs(?fromDate = fromDate, ?toDate = toDate), sprintf "Process %s logs" pid, false)

    /// Prints a detailed report for this process.
    member this.ShowInfo () = printf "%s" <| ProcessReporter.Report([proc.Value], "Process", false)

[<AutoSerializable(false)>]
/// Represents a cloud process.
type Process<'T> internal (config, pid : string, pmon : ProcessManager) = 
    inherit Process(config, pid, typeof<'T>, pmon) 

    override this.AwaitResultBoxed () : obj = this.AwaitResultBoxedAsync() |> Async.RunSync 
    override this.AwaitResultBoxedAsync () : Async<obj> =
        async {
            let rc : ResultCell<'T> = ResultCell.FromPath(config, this.ProcessEntity.Value.ResultUri)
            let! r = rc.AwaitResult()
            do! this.AwaitCompletionAsync()
            return r.Value :> obj
        }

    /// Awaits for the process result.
    member this.AwaitResult() : 'T = this.AwaitResultAsync() |> Async.RunSync
    /// Asynchronously waits for the process result.
    member this.AwaitResultAsync() : Async<'T> = 
        async {
            let rc : ResultCell<'T> = ResultCell.FromPath(config, this.ProcessEntity.Value.ResultUri)
            let! r = rc.AwaitResult()
            do! this.AwaitCompletionAsync()
            return r.Value
        }

    static member internal Create(config : ConfigurationId, pid : string, ty : Type, pmon : ProcessManager) : Process =
        let processT = typeof<Process<_>>.GetGenericTypeDefinition().MakeGenericType [| ty |]
        let flags = BindingFlags.NonPublic ||| BindingFlags.Instance
        let culture = System.Globalization.CultureInfo.InvariantCulture
        Activator.CreateInstance(processT, flags, null, [|config :> obj; pid :> obj ; pmon :> obj |], culture) :?> Process


[<AutoSerializable(false); Sealed>]
type internal ProcessCache () =
    static let cache = new ConcurrentDictionary<string, Process>()

    static member Add(ps : Process) = cache.TryAdd(ps.Id, ps) |> ignore
    static member TryGet(pid : string) = 
        match cache.TryGetValue(pid) with
        | true, p -> Some p
        | false, _ -> None