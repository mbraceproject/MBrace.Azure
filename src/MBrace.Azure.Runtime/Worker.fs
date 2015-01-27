namespace MBrace.Azure.Runtime

open System.Diagnostics
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common
open MBrace.Azure.Runtime.Resources
open MBrace.Continuation
open Nessos.FsPickler
open MBrace.Runtime.Vagrant
open MBrace.Store

type internal WorkerConfig = 
    { State              : RuntimeState
      MaxConcurrentTasks : int
      Resources          : ResourceRegistry
      Store              : ICloudFileStore
      Channel            : ICloudChannelProvider
      Atom               : ICloudAtomProvider
      Cache              : IObjectCache
      Logger             : ILogger
      WorkerMonitor      : WorkerMonitor
      ProcessMonitor     : ProcessMonitor }

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
        /// Used for tasks that cannot be UnPickled.
        let maxTaskDeliveryCount = 10


        let runTask (config : WorkerConfig) task deps faultCount  =
            let provider = RuntimeProvider.FromTask config.State config.WorkerMonitor deps task
            let info = task.ProcessInfo
            let serializer = config.Resources.Resolve<ISerializer>()
            let resources = resource { 
                yield! config.Resources
                yield { FileStore = defaultArg info.FileStore config.Store ; DefaultDirectory = info.DefaultDirectory; Cache = Some config.Cache; Serializer = serializer }
                yield { AtomProvider = defaultArg info.AtomProvider config.Atom ; DefaultContainer = info.DefaultAtomContainer }
                yield { ChannelProvider = defaultArg info.ChannelProvider config.Channel; DefaultContainer = info.DefaultChannelContainer }
            }
            Task.RunAsync provider resources deps faultCount task

        let run (config : WorkerConfig) (msg : QueueMessage) (task : Task) dependencies = async {
            let inline logf fmt = Printf.ksprintf config.Logger.Log fmt

            let! _ = Async.StartChild(msg.RenewLoopAsync())

            if task.TaskType = TaskType.Root then
                logf "Starting Root task for Process\nId:\"%s\"\nName:\"%s\"" task.ProcessInfo.Id task.ProcessInfo.Name
                do! config.ProcessMonitor.SetRunning(task.ProcessInfo.Id)

            if msg.DeliveryCount = 1 then
                do! config.ProcessMonitor.AddActiveTask(task.ProcessInfo.Id)

            logf "Starting task\n%s" (string task)
            let sw = new Stopwatch()
            sw.Start()
            let! result = Async.Catch(runTask config task dependencies (msg.DeliveryCount-1))
            sw.Stop()

            try
                match result with
                | Choice1Of2 () -> 
                    do! msg.CompleteAsync()
                    do! config.ProcessMonitor.AddCompletedTask(task.ProcessInfo.Id)
                    logf "Completed task\n%s\nTime:%O" (string task) sw.Elapsed
                | Choice2Of2 e -> 
                    do! msg.AbandonAsync()
                    do! config.ProcessMonitor.AddFaultedTask(task.ProcessInfo.Id)
                    logf "Task fault %s with:\n%O" (string task) e
            finally
                config.WorkerMonitor.DecrementTaskCount()
        }

        new MailboxProcessor<WorkerMessage>(fun inbox ->
            let rec workerLoop (state : WorkerState) = async {
                let! message = inbox.TryReceive(receiveTimeout)
                match message, state with
                | None, Running(config, _) ->
                    if config.WorkerMonitor.ActiveTasks >= config.MaxConcurrentTasks then
                        return! workerLoop state
                    else
                        let! task = Async.Catch <| config.State.TryDequeue()
                        match task with
                        | Choice1Of2 None -> return! workerLoop state
                        | Choice1Of2(Some msg) ->
                            let! task = Async.Catch <| async {
                                    config.Logger.Log "Got TaskItem."
                                    config.Logger.Logf "Message DeliveryCount : %d" msg.DeliveryCount
                                    let! ti = msg.GetPayloadAsync<TaskItem>()
                                    config.Logger.Log "Loading Dependencies."
                                    do! config.State.AssemblyManager.LoadDependencies ti.Dependencies
                                    config.Logger.Log "Task UnPickle."
                                    let task = VagrantRegistry.Pickler.UnPickleTyped<Task> ti.PickledTask
                                    return task, ti.Dependencies
                            } 
                            match task with
                            | Choice1Of2(task, deps) ->
                                config.WorkerMonitor.IncrementTaskCount()
                                let! _ = Async.StartChild(run config msg task deps)
                                ()
                            | Choice2Of2 ex ->
                                config.Logger.Logf "Failed to UnPickle task :\n%A" ex
                                if msg.DeliveryCount >= maxTaskDeliveryCount then
                                    // TODO : Set Process as Faulted.
                                    config.Logger.Logf "Faulted message : Complete."
                                    do! msg.CompleteAsync()
                                else
                                    config.Logger.Logf "Faulted message : Abandon."
                                    do! msg.AbandonAsync()
                            return! workerLoop state
                        | Choice2Of2 ex ->
                            config.Logger.Logf "Worker TaskQueue fault\n%A" ex
                            return! workerLoop state
                | None, Idle -> 
                    return! workerLoop state
                | Some(Start(config, handle)), Idle ->
                    return! workerLoop(Running(config, handle))
                | Some(Stop ch), Running(config, handle) ->
                    config.Logger.Log "Stop requested. Waiting for pending tasks."
                    let rec wait () = async {
                        if config.WorkerMonitor.ActiveTasks > 0 then
                            do! Async.Sleep receiveTimeout
                            return! wait ()
                    }
                    do! wait ()
                    config.Logger.Log "No active tasks."
                    config.Logger.Log "Unregister current worker."
                    do! config.WorkerMonitor.UnregisterCurrent()
                    config.Logger.Log "Worker stopped."
                    ch.Reply ()
                    handle.Reply()
                    return! workerLoop Idle
                | Some(Update(config,ch)), Running _ ->
                    config.Logger.Log "Updating configuration."
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