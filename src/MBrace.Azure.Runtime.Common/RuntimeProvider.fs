namespace Nessos.MBrace.Azure.Runtime

//
//  Implements the scheduling context for sample runtime.
//

#nowarn "444"

open System.Diagnostics

open Nessos.MBrace
open Nessos.MBrace.Library
open Nessos.MBrace.Runtime

open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Common
        
/// Scheduling implementation provider
type RuntimeProvider private (state : RuntimeState, procId, taskId, dependencies, context) =

    /// Creates a runtime provider instance for a provided task
    static member FromTask state procId dependencies (task : Task) =
        new RuntimeProvider(state, procId, task.TaskId, dependencies, Distributed)
        
    interface IRuntimeProvider with
        member __.ProcessId = procId
        member __.TaskId = taskId

        member __.SchedulingContext = context
        member __.WithSchedulingContext context = 
            new RuntimeProvider(state, procId, taskId, dependencies, context) :> IRuntimeProvider

        member __.ScheduleParallel computations = 
            match context with
            | Distributed -> Combinators.Parallel state procId dependencies computations
            | ThreadParallel -> ThreadPool.Parallel computations
            | Sequential -> Sequential.Parallel computations

        member __.ScheduleChoice computations = 
            match context with
            | Distributed -> Combinators.Choice state procId dependencies computations
            | ThreadParallel -> ThreadPool.Choice computations
            | Sequential -> Sequential.Choice computations

        member __.ScheduleStartChild(computation,_,_) =
            match context with
            | Distributed -> Combinators.StartChild state procId dependencies computation
            | ThreadParallel -> ThreadPool.StartChild computation
            | Sequential -> Sequential.StartChild computation

        member __.GetAvailableWorkers () = async { 
            let! ws = state.ResourceFactory.WorkerMonitor.GetWorkers()
            return ws |> Seq.map (fun w -> w :> IWorkerRef)
                      |> Seq.toArray
            }
        member __.CurrentWorker = state.ResourceFactory.WorkerMonitor.Current.AsWorkerRef() :> IWorkerRef
        member __.Logger = Unchecked.defaultof<_> //state.Logger :> ICloudLogger
