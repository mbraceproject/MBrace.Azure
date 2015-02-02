namespace MBrace.Azure.Runtime

#nowarn "444"

open MBrace
open MBrace.Runtime

open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common
open System
open MBrace.Runtime.InMemory
        
/// Scheduling implementation provider
type RuntimeProvider private (state : RuntimeState, wmon : WorkerManager, faultPolicy, taskId, psInfo, dependencies, context) =
        
    let failTargetWorker () = invalidOp <| sprintf "Cannot target worker when running in '%A' execution context" context

    let extractComputations (computations : seq<Cloud<_> * IWorkerRef option>) =
        computations
        |> Seq.map (fun (c,w) -> if Option.isSome w then failTargetWorker () else c)
        |> Seq.toArray

    /// Creates a runtime provider instance for a provided task
    static member FromTask state  wmon  dependencies (task : Task) =
        new RuntimeProvider(state, wmon, task.FaultPolicy, task.TaskId, task.ProcessInfo, dependencies, Distributed)

    interface ICloudRuntimeProvider with
        member __.ProcessId = psInfo.Id
        member __.TaskId = taskId

        member __.SchedulingContext = context
        member __.WithSchedulingContext ctx =
            match ctx, context with
            | Distributed, (ThreadParallel | Sequential)
            | ThreadParallel, Sequential ->
                invalidOp <| sprintf "Cannot set scheduling context to '%A' when it already is '%A'." ctx context
            | _ -> new RuntimeProvider(state, wmon, faultPolicy, taskId, psInfo, dependencies, ctx) :> ICloudRuntimeProvider

        member __.FaultPolicy = faultPolicy
        member __.WithFaultPolicy newPolicy = 
            new RuntimeProvider(state, wmon, newPolicy, taskId, psInfo, dependencies, context) :> ICloudRuntimeProvider

        member __.IsTargetedWorkerSupported = 
            match context with
            | Distributed -> true
            | _ -> false

        member __.ScheduleParallel computations = 
            match context with
            | Distributed -> Combinators.Parallel state psInfo dependencies faultPolicy computations
            | ThreadParallel -> ThreadPool.Parallel <| extractComputations computations
            | Sequential -> Sequential.Parallel <| extractComputations computations

        member __.ScheduleChoice computations = 
            match context with
            | Distributed -> Combinators.Choice state psInfo dependencies faultPolicy computations
            | ThreadParallel -> ThreadPool.Choice <| extractComputations computations
            | Sequential -> Sequential.Choice <| extractComputations computations

        member __.ScheduleStartChild(computation,wr,timeout) =
            if timeout.IsSome then raise <| NotImplementedException("StartChild with timeout")
            match context with
            | Distributed -> Combinators.StartChild state psInfo dependencies faultPolicy computation wr
            | ThreadParallel -> ThreadPool.StartChild computation
            | Sequential -> Sequential.StartChild computation

        member __.GetAvailableWorkers () = async { 
            let! ws = wmon.GetWorkerRefs(showInactive = false)
            return ws |> Seq.map (fun w -> w :> IWorkerRef)
                      |> Seq.toArray 
            }
        member __.CurrentWorker = wmon.Current.AsWorkerRef() :> IWorkerRef
        member __.Logger = state.ResourceFactory.RequestProcessLogger(psInfo.DefaultDirectory, psInfo.Id) :> ICloudLogger
