namespace MBrace.Azure.Runtime

open System.Diagnostics

open Nessos.FsPickler
open Nessos.Vagabond
open Nessos.Vagabond.AppDomainPool

open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities
open MBrace.Azure.Runtime.Primitives
open MBrace.Continuation
open MBrace.Runtime.Vagabond
open MBrace.Store
open MBrace.Runtime
open System
open System.Threading
open MBrace

type internal WorkerConfig = 
    { State                     : RuntimeState
      MaxConcurrentJobs         : int
      Resources                 : ResourceRegistry
      JobEvaluatorConfiguration : JobEvaluatorConfiguration
      Logger                    : ICloudLogger
      JobEvaluator              : JobEvaluator }

type private WorkerMessage =
    | Start of WorkerConfig  * AsyncReplyChannel<unit>
    | Update of WorkerConfig * AsyncReplyChannel<unit>
    | Stop of AsyncReplyChannel<unit>
    | IsActive of AsyncReplyChannel<bool>

type private WorkerState =
    | Idle
    | Running of WorkerConfig * AsyncReplyChannel<unit>

type internal Worker () =

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
            do! config.State.WorkerManager.UnregisterCurrent()
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
                            // run isolated job
                            let jc = Interlocked.Increment &currentJobCount
                            config.State.WorkerManager.SetJobCountLocal(jc)
                            config.Logger.Logf "Increase Dequeued Jobs %d" jc
                            let! _ = Async.StartChild <| async { 
                                try
                                    let! result = 
                                        Async.Catch <| async {
                                            config.Logger.Log "Downloading PickledJob"
                                            let! pickledJob = message.GetPayloadAsync<PickledJob>()
                                            config.Logger.Log "Downloading local assemblies"
                                            let! localAssemblies = config.State.AssemblyManager.DownloadDependencies pickledJob.Dependencies
                                            return pickledJob, localAssemblies
                                        }
                                    
                                    match result with
                                    | Choice1Of2(pickledJob, localAssemblies) ->                                    
                                        // evaluate job in AppDomain isolation
                                        let! ch = config.JobEvaluator.EvaluateAsync(config.JobEvaluatorConfiguration, localAssemblies, message, pickledJob)
                                        match ch with
                                        | Choice1Of2 () -> return ()
                                        | Choice2Of2 e  -> config.Logger.Logf "Internal Error : unhandled exception %A" e
                                    | Choice2Of2 ex ->
                                        config.Logger.Logf "Failed to download PickledJob or dependencies:\n%A" ex
//                                        config.Logger.Logf "SetResultUnsafe ResultCell %A" jobItem.ResultCell
//                                        let pk, rk = jobItem.ResultCell
//                                        do! ResultCell<obj>.SetResultUnsafe(jobItem.ConfigurationId, pk, rk, new FaultException(sprintf "Failed to unpickle Job '%s'" jobItem.JobId, ex))
//                                        let parentTaskCTS = jobItem.CancellationTokenSource
//                                        config.Logger.Logf "Cancel CancellationTokenSource %O" parentTaskCTS
//                                        parentTaskCTS.Cancel()
//                                        if jobItem.JobType = JobType.Root then
//                                            logf "Setting process Faulted"
//                                            do! staticConfiguration.State.ProcessManager.SetFaulted(jobItem.ProcessInfo.Id)
//                                        logf "Faulted message : Complete."
//                                        do! staticConfiguration.State.ProcessManager.AddFaultedJob(jobItem.ProcessInfo.Id)
//                                        do! staticConfiguration.State.JobQueue.CompleteAsync(msg)
                                        
                                finally
                                    let jc = Interlocked.Decrement &currentJobCount
                                    config.State.WorkerManager.SetJobCountLocal(jc)
                                    config.Logger.Logf "Decrease Dequeued Jobs %d" jc
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