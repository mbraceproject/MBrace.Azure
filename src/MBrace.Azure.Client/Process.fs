namespace MBrace.Azure.Client

open MBrace
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common
open MBrace.Azure.Runtime.Resources
open MBrace.Runtime
open MBrace.Continuation
open MBrace.Runtime.Compiler
open MBrace.Runtime.Utils.PrettyPrinters
open System
open System.IO
open System.Threading
open System.Reflection
open MBrace.Azure

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

    member internal __.ProcessEntity = proc
    member internal __.DistributedCancellationTokenSource = dcts.Value
    
    /// Awaits for the process result.
    abstract AwaitResultBoxed : unit -> obj
    /// Asynchronously waits for the process result.
    abstract AwaitResultBoxedAsync : unit -> Async<obj>

    /// Returns process' CancellationTokenSource.
    member __.CancellationTokenSource = dcts.Value :> ICloudCancellationTokenSource

    /// Process id.    
    member __.Id = pid

    /// Process name.
    member __.Name = proc.Value.Name

    /// Process type.
    member __.Type = ty

    /// Returns the initialization time for this process.
    member __.InitializationTime = let init = proc.Value.InitializationTime in init.ToLocalTime()
    
    /// Returns the execution time for this process.
    member __.ExecutionTime = 
        let s = 
            if proc.Value.Completed then proc.Value.CompletionTime
            else DateTimeOffset.UtcNow
        s - proc.Value.InitializationTime
    
    /// Returns iff the process is completed.
    member __.Completed = proc.Value.Completed

    /// Returns the number of tasks created by this process and are currently executing.
    member __.ActiveJobs = proc.Value.ActiveJobs

    /// Returns the number of tasks created by this process.
    member __.TotalJobs = proc.Value.TotalJobs

    /// Returns the number of tasks completed by this process.
    member __.CompletedJobs = proc.Value.CompletedJobs

    /// Returns the number of tasks failed to execute by this process.
    member __.FaultedJobs = proc.Value.FaultedJobs

    /// Sends a kill signal for this process.
    member __.Kill() = Async.RunSync(__.KillAsync())
    /// Asynchronously sends a kill signal for this process.
    member __.KillAsync() = async {
            do! pmon.SetKillRequested(pid)
            do! __.DistributedCancellationTokenSource.CancelAsync()
        }

    /// Asynchronously returns all cloud logs for this process.
    member __.GetLogsAsync(?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) = 
        logger.GetLogs(?fromDate = fromDate, ?toDate = toDate)
    /// Returns all cloud logs for this process.
    member __.GetLogs(?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) =
        Async.RunSync(__.GetLogsAsync(?fromDate = fromDate, ?toDate = toDate))

    /// Prints all cloud logs for this process.
    member __.ShowLogs(?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) = 
        printf "%s" <| LogReporter.Report(__.GetLogs(?fromDate = fromDate, ?toDate = toDate), sprintf "Process %s logs" pid, false)

    /// Prints a detailed report for this process.
    member __.ShowInfo () = printf "%s" <| ProcessReporter.Report([proc.Value], "Process", false)

[<AutoSerializable(false)>]
/// Represents a cloud process.
type Process<'T> internal (config, pid : string, pmon : ProcessManager) = 
    inherit Process(config, pid, typeof<'T>, pmon) 

    override __.AwaitResultBoxed () : obj =__.AwaitResultBoxedAsync() |> Async.RunSync 
    override __.AwaitResultBoxedAsync () : Async<obj> =
        async {
            let rc : ResultCell<'T> = ResultCell.FromPath(config, __.ProcessEntity.Value.ResultUri)
            let! r = rc.AwaitResult()
            return r.Value :> obj
        }

    /// Awaits for the process result.
    member __.AwaitResult() : 'T = __.AwaitResultAsync() |> Async.RunSync
    /// Asynchronously waits for the process result.
    member __.AwaitResultAsync() : Async<'T> = 
        async {
            let rc : ResultCell<'T> = ResultCell.FromPath(config, __.ProcessEntity.Value.ResultUri)
            let! r = rc.AwaitResult()
            return r.Value
        }

    static member internal Create(config : ConfigurationId, pid : string, ty : Type, pmon : ProcessManager) : Process =
        let processT = typeof<Process<_>>.GetGenericTypeDefinition().MakeGenericType [| ty |]
        let flags = BindingFlags.NonPublic ||| BindingFlags.Instance
        let culture = System.Globalization.CultureInfo.InvariantCulture
        Activator.CreateInstance(processT, flags, null, [|config :> obj; pid :> obj ; pmon :> obj |], culture) :?> Process
