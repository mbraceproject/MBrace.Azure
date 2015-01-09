namespace MBrace.Azure.Runtime

//
//  Implements the scheduling context for sample runtime.
//

#nowarn "444"

open System.Diagnostics

open MBrace
open MBrace.Runtime

open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common
open System
open MBrace.Continuation
open MBrace.Runtime.InMemory
        
/// Scheduling implementation provider
type RuntimeProvider private (state : RuntimeState, wmon : WorkerMonitor, faultPolicy, taskId, psInfo, dependencies, context) =

    /// Creates a runtime provider instance for a provided task
    static member FromTask state  wmon  dependencies (task : Task) =
        new RuntimeProvider(state, wmon, task.FaultPolicy, task.TaskId, task.ProcessInfo, dependencies, Distributed)
        
    interface IRuntimeProvider with
        member __.ProcessId = psInfo.Id
        member __.TaskId = taskId

        member __.SchedulingContext = context
        member __.WithSchedulingContext context = 
            new RuntimeProvider(state, wmon, faultPolicy, taskId, psInfo, dependencies, context) :> IRuntimeProvider

        member __.FaultPolicy = faultPolicy
        member __.WithFaultPolicy newPolicy = 
            new RuntimeProvider(state, wmon, newPolicy, taskId, psInfo, dependencies, context) :> IRuntimeProvider

        member __.ScheduleParallel computations = 
            match context with
            | Distributed -> Combinators.Parallel state psInfo dependencies faultPolicy computations
            | ThreadParallel -> ThreadPool.Parallel computations
            | Sequential -> Sequential.Parallel computations

        member __.ScheduleChoice computations = 
            match context with
            | Distributed -> Combinators.Choice state psInfo dependencies faultPolicy computations
            | ThreadParallel -> ThreadPool.Choice computations
            | Sequential -> Sequential.Choice computations

        member __.ScheduleStartChild(computation,wr,timeout) =
            if timeout.IsSome then raise <| NotImplementedException("StartChild with timeout")
            match context with
            | Distributed -> Combinators.StartChild state psInfo dependencies faultPolicy computation wr
            | ThreadParallel -> ThreadPool.StartChild computation
            | Sequential -> Sequential.StartChild computation

        member __.GetAvailableWorkers () = async { 
            let! ws = wmon.GetWorkerRefs()
            return ws |> Seq.map (fun w -> w :> IWorkerRef)
                      |> Seq.toArray 
            }
        member __.CurrentWorker = wmon.Current.AsWorkerRef() :> IWorkerRef
        member __.Logger = state.ResourceFactory.RequestProcessLogger(psInfo.DefaultDirectory, psInfo.Id) :> ICloudLogger
