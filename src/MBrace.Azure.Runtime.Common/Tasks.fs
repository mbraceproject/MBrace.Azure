namespace MBrace.Azure.Runtime

// Provides facility for the execution of tasks.
// In this context, a task denotes a single work item to be sent
// to a worker node for execution. Tasks may span multiple threads
// but are bound to a single process. A cloud workflow that has
// been passed continuations is a typical example of such a task.

#nowarn "0444" // MBrace.Core warnings

open System
open System.Threading.Tasks

open Nessos.Vagrant

open MBrace
open MBrace.Runtime
open MBrace.Azure.Runtime.Common
open MBrace.Runtime.Serialization
open MBrace.Azure.Runtime.Resources
open MBrace.Azure.Runtime.Common.Storage
open MBrace.Continuation
open Nessos.FsPickler
open MBrace.Runtime.Vagrant
open MBrace.Store

// Tasks are cloud workflows that have been attached to continuations.
// In that sense they are 'closed' multi-threaded computations that
// are difficult to reason about from a worker node's point of view.
// TaskExecutionMonitor provides a way to cooperatively track execution
// of such 'closed' computations.

/// Provides a mechanism for cooperative task execution monitoring.
[<AutoSerializable(false)>]
type TaskExecutionMonitor () =
    let tcs = TaskCompletionSource<unit> ()
    static let fromContext (ctx : ExecutionContext) = ctx.Resources.Resolve<TaskExecutionMonitor> ()

    member __.Task = tcs.Task
    member __.TriggerFault (e : exn) = tcs.TrySetException e |> ignore
    member __.TriggerCompletion () = tcs.TrySetResult () |> ignore

    /// Runs a single threaded, synchronous computation,
    /// triggering the contextual TaskExecutionMonitor on uncaught exception
    static member ProtectSync ctx (f : unit -> unit) : unit =
        let tem = fromContext ctx
        try f () with e -> tem.TriggerFault e |> ignore

    /// Runs an asynchronous computation,
    /// triggering the contextual TaskExecutionMonitor on uncaught exception
    static member ProtectAsync ctx (f : Async<unit>) : unit =
        let tem = fromContext ctx
        Async.StartWithContinuations(f, ignore, tem.TriggerFault, ignore)   

    /// Triggers task completion on the contextual TaskExecutionMonitor
    static member TriggerCompletion ctx =
        let tem = fromContext ctx in tem.TriggerCompletion () |> ignore

    /// Triggers task fault on the contextual TaskExecutionMonitor
    static member TriggerFault (ctx, e) =
        let tem = fromContext ctx in tem.TriggerFault e |> ignore

    /// Asynchronously await completion of provided TaskExecutionMonitor
    static member AwaitCompletion (tem : TaskExecutionMonitor) = async {
        try
            return! Async.AwaitTask tem.Task
        with :? System.AggregateException as e when e.InnerException <> null ->
            return! Async.Raise e.InnerException
    }

type TaskType = 
    | Root
    | Single
    | Parallel
    | Choice
    | Affined of affinity : string

/// Defines a task to be executed in a worker node
type Task = 
    {
        /// Return type of the defining cloud workflow.
        Type : Type
        /// Cloud process unique identifier
        ProcessId : string
        /// Task unique identifier
        TaskId : string
        /// Triggers task execution with worker-provided execution context
        StartTask : ExecutionContext -> unit
        /// Task fault policy
        FaultPolicy : FaultPolicy
        /// Exception Continuation
        Econt : ExecutionContext -> ExceptionDispatchInfo -> unit
        /// Distributed cancellation token source bound to task
        CancellationTokenSource : DistributedCancellationTokenSource
        /// Type of task.
        TaskType : TaskType
    }
with
    override this.ToString () =
        sprintf "%A/%s/%s : %s" this.TaskType this.ProcessId this.TaskId (Runtime.Utils.PrettyPrinters.Type.prettyPrint this.Type)

    /// <summary>
    ///     Asynchronously executes task in the local process.
    /// </summary>
    /// <param name="runtimeProvider">Local scheduler implementation.</param>
    /// <param name="dependencies">Task dependent assemblies.</param>
    /// <param name="task">Task to be executed.</param>
    static member RunAsync (runtimeProvider : IRuntimeProvider) 
                           (resources : ResourceRegistry)
                           (dependencies : AssemblyId list) 
                           (faultCount : int)
                           (task : Task) = async {
        let tem = new TaskExecutionMonitor()
        let ctx =
            {
                Resources = resource { 
                                yield runtimeProvider
                                yield! resources
                                yield tem
                                yield task.CancellationTokenSource
                                yield dependencies
                            }
                CancellationToken = task.CancellationTokenSource.GetLocalCancellationToken()
            }

        if faultCount > 0 then
            let faultException = new FaultException(sprintf "Fault exception when running task '%s'." task.TaskId)
            match task.FaultPolicy.Policy faultCount (faultException :> exn) with
            | None -> task.Econt ctx <| ExceptionDispatchInfo.Capture faultException
            | Some timeout ->
                do! Async.Sleep (int timeout.TotalMilliseconds)
                do task.StartTask ctx
        else
            do task.StartTask ctx

        return! TaskExecutionMonitor.AwaitCompletion tem
    }

/// TaskQueue message type.
type TaskItem = Pickle<Task> * string * AssemblyId list

/// Defines a handle to the state of a runtime instance.
type RuntimeState =
    {
        /// Reference to the global task queue employed by the runtime
        /// Queue contains pickled task and its vagrant dependency manifest
        TaskQueue : TaskQueue
        /// Reference to a Vagrant assembly exporting actor.
        AssemblyManager : AssemblyManager
        /// Reference to the runtime resource manager
        /// Used for generating latches, cancellation tokens and result cells.
        ResourceFactory : ResourceFactory
        /// Process monitoring.
        ProcessMonitor : ProcessMonitor
    }
with
    /// Initialize a new runtime state in the local process
    static member FromConfiguration (config : Configuration) = async {
        let! taskQueue = TaskQueue.Create(config.ConfigurationId, config.DefaultQueue, config.DefaultTopic)
        let assemblyManager = AssemblyManager.Create(config.ConfigurationId, config.DefaultTableOrContainer) 
        let resourceFactory = ResourceFactory.Create(config) 
        let pmon = ProcessMonitor.Create(config)
        return { TaskQueue = taskQueue; AssemblyManager = assemblyManager ; ResourceFactory = resourceFactory ; ProcessMonitor = pmon }
    }

    /// <summary>
    ///     Enqueue a cloud workflow with supplied continuations to the runtime task queue.
    /// </summary>
    /// <param name="dependencies">Vagrant dependency manifest.</param>
    /// <param name="cts">Distributed cancellation token source.</param>
    /// <param name="sc">Success continuation</param>
    /// <param name="ec">Exception continuation</param>
    /// <param name="cc">Cancellation continuation</param>
    /// <param name="wf">Workflow</param>
    /// <param name="affinity">Optional task affinity.</param>
    member rt.EnqueueTask(procId, dependencies, cts, fp, sc, ec, cc, wf : Cloud<'T>, taskType : TaskType) : Async<unit> =
        let taskId = guid()
        let startTask ctx =
            let cont = { Success = sc ; Exception = ec ; Cancellation = cc }
            Cloud.StartWithContinuations(wf, cont, ctx)
        let affinity = match taskType with Affined a -> Some a | _ -> None
        let task = 
            { 
                Type = typeof<'T>
                ProcessId = procId
                TaskId = taskId
                StartTask = startTask
                CancellationTokenSource = cts
                FaultPolicy = fp
                Econt = ec
                TaskType = taskType
            }
        
        let taskp = VagrantRegistry.Pickler.PickleTyped task
        let taskItem = (taskp, procId, dependencies)
        async {
            do! rt.TaskQueue.Enqueue<TaskItem>(taskItem, ?affinity = affinity)
            do! rt.ProcessMonitor.IncreaseTotalTasks(procId)
        }

    /// <summary>
    ///     Enqueue a batch of cloud workflows with supplied continuations to the runtime task queue.
    ///     Used for Parallel and Choice combinators
    /// </summary>
    /// <param name="dependencies">Vagrant dependency manifest.</param>
    /// <param name="cts">Distributed cancellation token source.</param>
    /// <param name="scFactory">Success continuation factory.</param>
    /// <param name="ec">Exception continuation.</param>
    /// <param name="cc">Cancellation continuation.</param>
    /// <param name="wfs">Workflows</param>
    /// <param name="affinity">Optional task affinity.</param>
    member rt.EnqueueTaskBatch(procId, dependencies, cts, fp, scFactory, ec, cc, wfs : Cloud<'T> [], taskType) : Async<unit> =
        let tasks = Array.zeroCreate wfs.Length
        for i = 0 to wfs.Length - 1 do
            let taskId = guid()
            let startTask ctx =
                let cont = { Success = scFactory i ; Exception = ec ; Cancellation = cc }
                Cloud.StartWithContinuations(wfs.[i], cont, ctx)
            let task = 
                { 
                    Type = typeof<'T>
                    ProcessId = procId
                    TaskId = taskId
                    StartTask = startTask
                    CancellationTokenSource = cts
                    FaultPolicy = fp
                    Econt = ec
                    TaskType = taskType
                }

            let taskp = VagrantRegistry.Pickler.PickleTyped task
            tasks.[i] <- (taskp, procId, dependencies)
        async {
            do! rt.TaskQueue.EnqueueBatch<TaskItem>(tasks)
            do! rt.ProcessMonitor.IncreaseTotalTasks(procId, tasks.Length)
        }

    /// <summary>
    ///     Schedules a cloud workflow as a distributed result cell.
    ///     Used for child tasks.
    /// </summary>
    /// <param name="dependencies">Declared workflow dependencies.</param>
    /// <param name="cts">Cancellation token source bound to task.</param>
    /// <param name="wf">Input workflow.</param>
    member rt.StartAsCell(procId, dependencies, cts, fp, wf : Cloud<'T>, taskType) = async {
        let! resultCell = rt.ResourceFactory.RequestResultCell<'T>(processIdToStorageId procId)
        let setResult ctx r = 
            async {
                let! success = resultCell.SetResult r
                TaskExecutionMonitor.TriggerCompletion ctx
            } |> TaskExecutionMonitor.ProtectAsync ctx

        let scont ctx t = setResult ctx (Completed t)
        let econt ctx e = setResult ctx (Exception e)
        let ccont ctx c = setResult ctx (Cancelled c)
        do! rt.EnqueueTask(procId, dependencies, cts, fp, scont, econt, ccont, wf, taskType)
        return resultCell
    }

    /// <summary>
    ///     Schedules a cloud workflow as a distributed result cell.
    ///     Used for root-level workflows.
    /// </summary>
    /// <param name="dependencies">Declared workflow dependencies.</param>
    /// <param name="cts">Cancellation token source bound to task.</param>
    /// <param name="wf">Input workflow.</param>
    /// <param name="name">Process name.</param>
    /// <param name="procId">Process id.</param>
    member rt.StartAsProcess(procId, name, dependencies, (cts : DistributedCancellationTokenSource), fp, wf : Cloud<'T>) = async {
        let! resultCell = rt.ResourceFactory.RequestResultCell<'T>(processIdToStorageId procId)

        let! _ = rt.ProcessMonitor
                   .CreateRecord( procId, name, typeof<'T>, dependencies,
                                    string cts.Uri, 
                                    string resultCell.Uri)

        let setResult ctx r = 
            async {
                let! success = resultCell.SetResult r
                let pmon = ctx.Resources.Resolve<ProcessMonitor>()
                match r with
                | Completed _ 
                | Exception _ -> do! pmon.SetCompleted(procId)
                | Cancelled _ -> do! pmon.SetKilled(procId)
                TaskExecutionMonitor.TriggerCompletion ctx
            } |> TaskExecutionMonitor.ProtectAsync ctx

        let scont ctx t = setResult ctx (Completed t)
        let econt ctx e = setResult ctx (Exception e)
        let ccont ctx c = setResult ctx (Cancelled c)
        do! rt.EnqueueTask(procId, dependencies, cts, fp, scont, econt, ccont, wf, TaskType.Root)
        return resultCell
    }

    /// Attempt to dequeue a task from the runtime task queue
    member rt.TryDequeue () = async {
        let! item = rt.TaskQueue.TryDequeue()

        match item with
        | None -> return None
        | Some msg -> 
            let! (tp, procId, deps) = msg.GetPayloadAsync<TaskItem>()
            do! rt.AssemblyManager.LoadDependencies deps
            let task = VagrantRegistry.Pickler.UnPickleTyped<Task> tp
            return Some (msg, task, procId, deps)
    }