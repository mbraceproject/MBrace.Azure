namespace MBrace.Azure.Runtime

open System.Diagnostics

open Nessos.FsPickler
open Nessos.Vagabond
open Nessos.Vagabond.AppDomainPool

open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common
open MBrace.Azure.Runtime.Resources
open MBrace.Continuation
open MBrace.Runtime.Vagabond
open MBrace.Store
open MBrace.Runtime
open System
open System.Threading

type private WorkerMessage =
    | Start of WorkerConfig  * AsyncReplyChannel<unit>
    | Update of WorkerConfig * AsyncReplyChannel<unit>
    | Stop of AsyncReplyChannel<unit>
    | IsActive of AsyncReplyChannel<bool>

type private WorkerState =
    | Idle
    | Running of WorkerConfig * AsyncReplyChannel<unit>

type internal Worker (configuration : Configuration, serviceId : string) =

    let jobEvaluator =  lazy new JobEvaluator(configuration, serviceId)
    
    let mutable currentJobCount = 0
    
    let workerLoopAgent =
        /// Timeout for the Mailbox loop.
        let receiveTimeout = 100
        /// Sleep time for runtime errors.
        let onErrorWaitTime = 5000


        let waitForPendingJobs (config : WorkerConfig) = async {
            config.Logger.Log "Stop requested. Waiting for pending jobs."
            let rec wait () = async {
                if currentJobCount > 0 then
                    do! Async.Sleep receiveTimeout
                    return! wait ()
            }
            do! wait ()
            config.Logger.Log "No active jobs."
            config.Logger.Log "Unregister current worker."
            do! config.WorkerManager.UnregisterCurrent()
            config.Logger.Log "Worker stopped."
        }

        new MailboxProcessor<WorkerMessage>(fun inbox ->
            let rec workerLoop (state : WorkerState) = async {
                let! message = async {
                    if inbox.CurrentQueueLength > 0 then 
                        return! inbox.TryReceive()
                    else return None
                }
                match message, state with
                | None, Running(config, _) ->
                    if currentJobCount >= config.MaxConcurrentJobs then
                        do! Async.Sleep receiveTimeout
                        return! workerLoop state
                    else
                        let! job = Async.Catch <| config.State.TryDequeue()
                        match job with
                        | Choice1Of2 None -> 
                            return! workerLoop state
                        | Choice1Of2(Some message) ->
                            let _ = Interlocked.Increment &currentJobCount
                            let! _ = Async.StartChild <| async { 
                                    let! _ = config.WorkerManager.IncrementJobCount()
                                    let! _ = jobEvaluator.Value.EvaluateAsync(config.JobEvaluatorConfiguration, message)
                                    let  _ = Interlocked.Decrement &currentJobCount
                                    let! _ = config.WorkerManager.DecrementJobCount()
                                    return ()
                            }
                            return! workerLoop state
                        | Choice2Of2 ex ->
                            config.Logger.Logf "Worker JobQueue fault\n%A" ex
                            do! Async.Sleep onErrorWaitTime
                            return! workerLoop state
                | None, Idle -> 
                    do! Async.Sleep receiveTimeout
                    return! workerLoop state
                | Some(Start(config, handle)), Idle ->
                    return! workerLoop(Running(config, handle))
                | Some(Stop ch), Running(config, handle) ->
                    do! waitForPendingJobs config
                    ch.Reply ()
                    handle.Reply()
                    return! workerLoop Idle
                | Some(Update(config,ch)), Running _ ->
                    config.Logger.Log "Updating worker configuration."
                    return! workerLoop(Running(config, ch))
                | Some(IsActive ch), Idle ->
                    ch.Reply(false)
                    return! workerLoop state
                | Some(IsActive ch), Running _ ->
                    ch.Reply(true)
                    return! workerLoop state
                | Some(Start _), _  ->
                    return invalidOp "Called Start, but worker is not Idle."
                | _, Idle ->
                    return invalidOp "Worker is Idle."
            }
            workerLoop Idle
        )

    do workerLoopAgent.Start()

    member __.IsActive = workerLoopAgent.PostAndReply(IsActive)

    member __.Start(configuration : WorkerConfig) =
        workerLoopAgent.PostAndReply(fun ch -> Start(configuration, ch) )
        
    member __.Stop() =
        workerLoopAgent.PostAndReply(fun ch -> Stop(ch))

    member __.Restart(configuration) =
        __.Stop()
        __.Start(configuration)

    member __.InitializeJobEvaluator () = jobEvaluator.Force() |> ignore