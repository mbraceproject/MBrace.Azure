module Nessos.MBrace.Azure.Runtime.Combinators

//
//  Provides distributed implementations for Cloud.Parallel, Cloud.Choice and Cloud.StartChild
//

open Nessos.MBrace
open Nessos.MBrace.Runtime
open Nessos.MBrace.Azure.Runtime

#nowarn "444"

open Nessos.MBrace.Azure.Runtime.Resources
open Nessos.MBrace.Azure.Runtime.Common.Storage

let inline private withCancellationToken (cts : DistributedCancellationTokenSource) (ctx : ExecutionContext) =
    let token = cts.GetLocalCancellationToken()
    { Resources = ctx.Resources.Register(cts) ; CancellationToken = token }

let private asyncFromContinuations f =
    Cloud.FromContinuations(fun ctx cont -> TaskExecutionMonitor.ProtectAsync ctx (f ctx cont))
        
let Parallel (state : RuntimeState) procId dependencies (computations : seq<Cloud<'T>>) =
    asyncFromContinuations(fun ctx cont -> async {
        match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
        | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
        | Choice1Of2 [||] -> cont.Success ctx [||]
        // schedule single-child parallel workflows in current task
        // note that this invalidates expected workflow semantics w.r.t. mutability.
        | Choice1Of2 [| comp |] ->
            let cont' = Continuation.map (fun t -> [| t |]) cont
            Cloud.StartWithContinuations(comp, cont', ctx)

        | Choice1Of2 computations ->
            // request runtime resources required for distribution coordination
            let storageId = processIdToStorageId procId
            let currentCts = ctx.Resources.Resolve<DistributedCancellationTokenSource> ()
            let! childCts = state.ResourceFactory.RequestCancellationTokenSource(storageId, parent = currentCts)
            let! resultAggregator = state.ResourceFactory.RequestResultAggregator<'T>(storageId, computations.Length)
            let! cancellationLatch = state.ResourceFactory.RequestCounter(storageId, 0)

            let onSuccess i ctx (t : 'T) = 
                async {
                    let! isCompleted = resultAggregator.SetResult(i, t)
                    if isCompleted then 
                        // this is the last child callback, aggregate result and call parent continuation
                        let! results = resultAggregator.ToArray()
                        childCts.Cancel()
                        cont.Success (withCancellationToken currentCts ctx) results
                    else // results pending, declare task completed.
                        TaskExecutionMonitor.TriggerCompletion ctx
                } |> TaskExecutionMonitor.ProtectAsync ctx

            let onException ctx e =
                async {
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then // is first task to request workflow cancellation, grant access
                        childCts.Cancel()
                        cont.Exception (withCancellationToken currentCts ctx) e
                    else // cancellation already triggered by different party, declare task completed.
                        TaskExecutionMonitor.TriggerCompletion ctx
                } |> TaskExecutionMonitor.ProtectAsync ctx

            let onCancellation ctx c =
                async {
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then // is first task to request workflow cancellation, grant access
                        childCts.Cancel()
                        cont.Cancellation ctx c
                    else // cancellation already triggered by different party, declare task completed.
                        TaskExecutionMonitor.TriggerCompletion ctx
                } |> TaskExecutionMonitor.ProtectAsync ctx

            try
                state.EnqueueTaskBatch procId dependencies childCts onSuccess onException onCancellation computations
            with e ->
                childCts.Cancel() ; return! Async.Raise e
                    
            TaskExecutionMonitor.TriggerCompletion ctx })

let Choice (state : RuntimeState) procId dependencies (computations : seq<Cloud<'T option>>) =
    asyncFromContinuations(fun ctx cont -> async {
        match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
        | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
        | Choice1Of2 [||] -> cont.Success ctx None
        // schedule single-child parallel workflows in current task
        // note that this invalidates expected workflow semantics w.r.t. mutability.
        | Choice1Of2 [| comp |] -> Cloud.StartWithContinuations(comp, cont, ctx)
        | Choice1Of2 computations ->
            // request runtime resources required for distribution coordination
            let storageId = processIdToStorageId procId
            let n = computations.Length // avoid capturing computation array in cont closures
            let currentCts = ctx.Resources.Resolve<DistributedCancellationTokenSource>()
            let! childCts = state.ResourceFactory.RequestCancellationTokenSource(storageId, currentCts)
            let! completionLatch = state.ResourceFactory.RequestCounter(storageId, 0)
            let! cancellationLatch = state.ResourceFactory.RequestCounter(storageId, 0)

            let onSuccess ctx (topt : 'T option) =
                async {
                    if Option.isSome topt then // 'Some' result, attempt to complete workflow
                        let! latchCount = cancellationLatch.Increment()
                        if latchCount = 1 then 
                            // first child to initiate cancellation, grant access to parent scont
                            childCts.Cancel ()
                            cont.Success (withCancellationToken currentCts ctx) topt
                        else
                            // workflow already cancelled, declare task completion
                            TaskExecutionMonitor.TriggerCompletion ctx
                    else
                        // 'None', increment completion latch
                        let! completionCount = completionLatch.Increment ()
                        if completionCount = n then 
                            // is last task to complete with 'None', pass None to parent scont
                            childCts.Cancel()
                            cont.Success (withCancellationToken currentCts ctx) None
                        else
                            // other tasks pending, declare task completion
                            TaskExecutionMonitor.TriggerCompletion ctx
                } |> TaskExecutionMonitor.ProtectAsync ctx

            let onException ctx e =
                async {
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then // is first task to request workflow cancellation, grant access
                        childCts.Cancel ()
                        cont.Exception (withCancellationToken currentCts ctx) e
                    else // cancellation already triggered by different party, declare task completed.
                        TaskExecutionMonitor.TriggerCompletion ctx
                } |> TaskExecutionMonitor.ProtectAsync ctx

            let onCancellation ctx c =
                async {
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then // is first task to request workflow cancellation, grant access
                        childCts.Cancel()
                        cont.Cancellation (withCancellationToken currentCts ctx) c
                    else // cancellation already triggered by different party, declare task completed.
                        TaskExecutionMonitor.TriggerCompletion ctx
                } |> TaskExecutionMonitor.ProtectAsync ctx

            try
                state.EnqueueTaskBatch procId dependencies childCts (fun _ -> onSuccess) onException onCancellation computations
            with e ->
                childCts.Cancel() ; return! Async.Raise e
                    
            TaskExecutionMonitor.TriggerCompletion ctx })


let StartChild (state : RuntimeState) procId dependencies (computation : Cloud<'T>) = cloud {
    let! cts = Cloud.GetResource<DistributedCancellationTokenSource> ()
    let! resultCell = Cloud.OfAsync <| state.StartAsCell procId dependencies cts computation
    return cloud { 
        let! result = Cloud.OfAsync <| resultCell.AwaitResult() 
        return result.Value
    }
}