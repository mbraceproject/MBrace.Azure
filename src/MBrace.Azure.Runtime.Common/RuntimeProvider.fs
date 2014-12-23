namespace Nessos.MBrace.Azure.Runtime

//
//  Implements the scheduling context for sample runtime.
//

#nowarn "444"

open System.Diagnostics

open Nessos.MBrace
open Nessos.MBrace.Runtime

open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Common
open System
open Nessos.MBrace.Continuation
open Nessos.MBrace.Runtime.InMemory
        
/// Scheduling implementation provider
type RuntimeProvider private (state : RuntimeState, wmon : WorkerMonitor, procId, faultPolicy, taskId, dependencies, context) =

    /// Creates a runtime provider instance for a provided task
    static member FromTask state  wmon  procId dependencies (task : Task) =
        new RuntimeProvider(state, wmon, procId, task.FaultPolicy, task.TaskId, dependencies, Distributed)
        
    interface IRuntimeProvider with
        member __.ProcessId = procId
        member __.TaskId = taskId

        member __.SchedulingContext = context
        member __.WithSchedulingContext context = 
            new RuntimeProvider(state, wmon, procId, faultPolicy, taskId, dependencies, context) :> IRuntimeProvider

        member __.FaultPolicy = faultPolicy
        member __.WithFaultPolicy newPolicy = 
            new RuntimeProvider(state, wmon, procId, newPolicy, taskId, dependencies, context) :> IRuntimeProvider

        member __.ScheduleParallel computations = 
            match context with
            | Distributed -> Combinators.Parallel state procId dependencies faultPolicy computations
            | ThreadParallel -> ThreadPool.Parallel computations
            | Sequential -> Sequential.Parallel computations

        member __.ScheduleChoice computations = 
            match context with
            | Distributed -> Combinators.Choice state procId dependencies faultPolicy computations
            | ThreadParallel -> ThreadPool.Choice computations
            | Sequential -> Sequential.Choice computations

        member __.ScheduleStartChild(computation,wr,timeout) =
            if timeout.IsSome then raise <| NotImplementedException("StartChild with timeout")
            match context with
            | Distributed -> Combinators.StartChild state procId dependencies faultPolicy computation wr
            | ThreadParallel -> ThreadPool.StartChild computation
            | Sequential -> Sequential.StartChild computation

        member __.GetAvailableWorkers () = async { 
            let! ws = wmon.GetWorkerRefs()
            return ws |> Seq.map (fun w -> w :> IWorkerRef)
                      |> Seq.toArray 
            }
        member __.CurrentWorker = wmon.Current.AsWorkerRef() :> IWorkerRef
        member __.Logger = state.ResourceFactory.RequestProcessLogger(Storage.processIdToStorageId procId, procId) :> ICloudLogger
