namespace MBrace.Azure.Runtime

open System.Diagnostics

open Nessos.FsPickler
open Nessos.Vagabond
open Nessos.Vagabond.AppDomainPool

open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common
open MBrace.Azure.Runtime.Resources
open MBrace.Continuation
open MBrace.Runtime.Vagabond
open MBrace.Store
open MBrace.Runtime
open System

type internal WorkerConfig = 
    { State              : RuntimeState
      MaxConcurrentJobs  : int
      Resources          : ResourceRegistry
      Store              : ICloudFileStore
      Channel            : ICloudChannelProvider
      Atom               : ICloudAtomProvider
      Cache              : IObjectCache
      Logger             : ICloudLogger
      WorkerMonitor      : WorkerManager
      ProcessMonitor     : ProcessManager }

type internal WorkerMessage =
    | Start of WorkerConfig  * AsyncReplyChannel<unit>
    | Update of WorkerConfig * AsyncReplyChannel<unit>
    | Stop of AsyncReplyChannel<unit>
    | IsActive of AsyncReplyChannel<bool>

type private WorkerState =
    | Idle
    | Running of WorkerConfig * AsyncReplyChannel<unit>

type internal Worker () =

    let workerLoopAgent =
        /// Timeout for the Mailbox loop.
        let receiveTimeout = 100
        /// Used for jobs that cannot be UnPickled.
        let maxJobDeliveryCount = 1
        /// Sleep time for runtime errors.
        let onErrorWaitTime = 5000

        let runJob (config : WorkerConfig) job deps faultCount  =
            let provider = RuntimeProvider.FromJob config.State config.WorkerMonitor deps job
            let info = job.ProcessInfo
            let serializer = config.Resources.Resolve<ISerializer>()
            let resources = resource { 
                yield! config.Resources
                yield { FileStore = defaultArg info.FileStore config.Store ; DefaultDirectory = info.DefaultDirectory; Cache = Some config.Cache; Serializer = serializer }
                yield { AtomProvider = defaultArg info.AtomProvider config.Atom ; DefaultContainer = info.DefaultAtomContainer }
                yield { ChannelProvider = defaultArg info.ChannelProvider config.Channel; DefaultContainer = info.DefaultChannelContainer }
            }
            Job.RunAsync provider resources faultCount job

        let run (config : WorkerConfig) (msg : QueueMessage) (job : Job) dependencies = async {
            let inline logf fmt = Printf.ksprintf config.Logger.Log fmt

            let! _ = Async.StartChild(msg.RenewLoopAsync())

            if job.JobType = JobType.Root then
                logf "Starting Root job for Process Id : %s, Name : %s" job.ProcessInfo.Id job.ProcessInfo.Name
                do! config.ProcessMonitor.SetRunning(job.ProcessInfo.Id)

            if msg.DeliveryCount = 1 then
                do! config.ProcessMonitor.AddActiveJob(job.ProcessInfo.Id)

            logf "Starting job\n%s" (string job)
            let sw = new Stopwatch()
            sw.Start()
            let! result = Async.Catch(runJob config job dependencies (msg.DeliveryCount-1))
            sw.Stop()

            try
                match result with
                | Choice1Of2 () -> 
                    do! msg.CompleteAsync()
                    do! config.ProcessMonitor.AddCompletedJob(job.ProcessInfo.Id)
                    logf "Completed job\n%s\nTime : %O" (string job) sw.Elapsed
                | Choice2Of2 e -> 
                    do! msg.AbandonAsync()
                    do! config.ProcessMonitor.AddFaultedJob(job.ProcessInfo.Id)
                    logf "Job fault %s with :\n%O" (string job) e
            finally
                config.WorkerMonitor.DecrementJobCount()
                config.Logger.Logf "ActiveJobs : %d" config.WorkerMonitor.ActiveJobs
        }

        let waitForPendingJobs (config : WorkerConfig) = async {
            config.Logger.Log "Stop requested. Waiting for pending jobs."
            let rec wait () = async {
                if config.WorkerMonitor.ActiveJobs > 0 then
                    do! Async.Sleep receiveTimeout
                    return! wait ()
            }
            do! wait ()
            config.Logger.Log "No active jobs."
            config.Logger.Log "Unregister current worker."
            do! config.WorkerMonitor.UnregisterCurrent()
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
                    if config.WorkerMonitor.ActiveJobs >= config.MaxConcurrentJobs then
                        return! workerLoop state
                    else
                        let! job = Async.Catch <| config.State.TryDequeue()
                        match job with
                        | Choice1Of2 None -> return! workerLoop state
                        | Choice1Of2(Some msg) ->
                            let! job = Async.Catch <| async {
                                config.Logger.Log "Got JobItem."
                                config.Logger.Logf "Message DeliveryCount : %d" msg.DeliveryCount
                                let! ti = msg.GetPayloadAsync<JobItem>()
                                do! config.State.AssemblyManager.LoadDependencies ti.Dependencies
                                config.Logger.Logf "Job UnPickle [%d bytes]." ti.PickledJob.Bytes.Length
                                let job = VagabondRegistry.Instance.Pickler.UnPickleTyped<Job> ti.PickledJob
                                return job, ti.Dependencies
                            } 
                            match job with
                            | Choice1Of2(job, deps) ->
                                config.WorkerMonitor.IncrementJobCount()
                                config.Logger.Logf "ActiveJobs : %d" config.WorkerMonitor.ActiveJobs
                                let! _ = Async.StartChild(run config msg job deps)
                                ()
                            | Choice2Of2 ex ->
                                config.Logger.Logf "Failed to UnPickle Job :\n%A" ex
                                if msg.DeliveryCount >= maxJobDeliveryCount then
                                    // TODO : Set Process as Faulted.
                                    config.Logger.Logf "Faulted message : Complete."
                                    do! msg.CompleteAsync()
                                else
                                    config.Logger.Logf "Faulted message : Abandon."
                                    do! msg.AbandonAsync()
                                do! Async.Sleep onErrorWaitTime
                            return! workerLoop state
                        | Choice2Of2 ex ->
                            config.Logger.Logf "Worker JobQueue fault\n%A" ex
                            do! Async.Sleep onErrorWaitTime
                            return! workerLoop state
                | None, Idle -> 
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