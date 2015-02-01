namespace MBrace.Azure.Runtime

// Provides facility for the execution of tasks.
// In this context, a task denotes a single work item to be sent
// to a worker node for execution. Tasks may span multiple threads
// but are bound to a single process. A cloud workflow that has
// been passed continuations is a typical example of such a task.

#nowarn "0444" // MBrace.Core warnings

open System
open System.Threading.Tasks

open Nessos.Vagabond

open MBrace
open MBrace.Azure.Runtime.Common
open MBrace.Azure.Runtime.Resources
open MBrace.Continuation
open Nessos.FsPickler
open MBrace.Runtime.Vagabond
open MBrace.Store
open MBrace.Runtime

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


// TODO : Merge with ProcessRecord.
/// Process information record.
type ProcessInfo =
    {
        /// Cloud process unique identifier.
        Id : string
        /// Process name.
        Name : string

        /// Default file store container for process.
        DefaultDirectory : string
        /// Default atom container for process.
        DefaultAtomContainer : string
        /// Default channel container for process.
        DefaultChannelContainer : string

        /// Optional filestore for this process.
        FileStore : ICloudFileStore option
        /// Optional atom provider for this process.
        AtomProvider : ICloudAtomProvider option
        /// Optional channel provider for this process.
        ChannelProvider : ICloudChannelProvider option
    }

/// Task kind.
type TaskType =
    /// Root task for process.
    | Root
    /// Task created by Cloud.StartChild.
    | StartChild
    /// Task created by Cloud.StartChild with affinity.
    | Affined of affinity : string
    /// Task created by Cloud.Parallel.
    | Parallel
    /// Task created by Cloud.Choice.
    | Choice
    /// Task created by Cloud.Parallel with affinity.
    | ParallelAffined of affinity : string
    /// Task created by Cloud.Choice with affinity.
    | ChoiceAffined of affinity : string

/// Defines a task to be executed in a worker node
type Task = 
    {
        /// Return type of the defining cloud workflow.
        Type : Type
        /// Process information record.
        ProcessInfo : ProcessInfo
        /// Task unique identifier.
        TaskId : string
        /// Triggers task execution with worker-provided execution context.
        StartTask : ExecutionContext -> unit
        /// Task fault policy.
        FaultPolicy : FaultPolicy
        /// Exception Continuation.
        Econt : ExecutionContext -> ExceptionDispatchInfo -> unit
        /// Distributed cancellation token source bound to task.
        CancellationTokenSource : DistributedCancellationTokenSource
        /// Type of task.
        TaskType : TaskType
    }
with
    override this.ToString () =
        sprintf "TaskType:\"%A\"\nProcess:\"%s\"\nTaskId:\"%s\"\nReturnType:\"%s\" " this.TaskType this.ProcessInfo.Id this.TaskId (Runtime.Utils.PrettyPrinters.Type.prettyPrint this.Type)

    /// <summary>
    ///     Asynchronously executes task in the local process.
    /// </summary>
    /// <param name="runtimeProvider">Local scheduler implementation.</param>
    /// <param name="dependencies">Task dependent assemblies.</param>
    /// <param name="task">Task to be executed.</param>
    static member RunAsync (runtimeProvider : ICloudRuntimeProvider) 
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
type TaskItem = 
    { PickledTask : Pickle<Task>
      Dependencies : AssemblyId list }

/// Defines a handle to the state of a runtime instance.
type RuntimeState =
    {
        /// Reference to the global task queue employed by the runtime
        /// Queue contains pickled task and its vagrant dependency manifest
        TaskQueue : TaskQueue
        /// Assembly exporting facility.
        AssemblyManager : AssemblyManager
        /// Reference to the runtime resource manager
        /// Used for generating latches, cancellation tokens and result cells.
        ResourceFactory : ResourceFactory
        /// Process monitoring.
        ProcessMonitor : ProcessManager
    }
with
    /// Initialize a new runtime state in the local process
    static member FromConfiguration (config : Configuration) = async {
        let! taskQueue = TaskQueue.Create(config.ConfigurationId, config.DefaultQueue, config.DefaultTopic)
        let assemblyManager = AssemblyManager.Create(config.ConfigurationId, config.DefaultTableOrContainer) 
        let resourceFactory = ResourceFactory.Create(config) 
        let pmon = ProcessManager.Create(config)
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
    member rt.EnqueueTask(psInfo, dependencies, cts, fp, sc, ec, cc, wf : Cloud<'T>, taskType : TaskType) : Async<unit> =
        let taskId = guid()
        let startTask ctx =
            let cont = { Success = sc ; Exception = ec ; Cancellation = cc }
            Cloud.StartWithContinuations(wf, cont, ctx)
        let affinity = match taskType with Affined a -> Some a | _ -> None
        let task = 
            { 
                Type = typeof<'T>
                ProcessInfo = psInfo
                TaskId = taskId
                StartTask = startTask
                CancellationTokenSource = cts
                FaultPolicy = fp
                Econt = ec
                TaskType = taskType
            }
        
        let taskp = VagabondRegistry.Pickler.PickleTyped task
        let taskItem = { PickledTask = taskp; Dependencies = dependencies }
        async {
            do! rt.TaskQueue.Enqueue<TaskItem>(taskItem, ?affinity = affinity)
            do! rt.ProcessMonitor.IncreaseTotalTasks(psInfo.Id)
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
    member rt.EnqueueTaskBatch(psInfo, dependencies, cts, fp, scFactory, ec, cc, wfs : (Cloud<'T> * IWorkerRef option) [], taskType) : Async<unit> =
        let tasks = Array.zeroCreate wfs.Length
        for i = 0 to wfs.Length - 1 do
            let taskId = guid()
            let startTask ctx =
                let cont = { Success = scFactory i ; Exception = ec ; Cancellation = cc }
                Cloud.StartWithContinuations(fst wfs.[i], cont, ctx)
            let taskType aff  =
                match taskType, aff with
                | (Parallel | Choice) as t, None -> t
                | Parallel, Some a -> ParallelAffined a
                | Choice, Some a -> ChoiceAffined a
                | t -> failwith "Invalid TaskType %A in EnqueueBatch." t

            let affinity = match snd wfs.[i] with Some wr -> Some wr.Id | None -> None
            let task = 
                { 
                    Type = typeof<'T>
                    ProcessInfo = psInfo
                    TaskId = taskId
                    StartTask = startTask
                    CancellationTokenSource = cts
                    FaultPolicy = fp
                    Econt = ec
                    TaskType = taskType affinity
                }

            let taskp = VagabondRegistry.Pickler.PickleTyped task
            tasks.[i] <- { PickledTask = taskp; Dependencies = dependencies }, affinity
        async {
            do! rt.TaskQueue.EnqueueBatch<TaskItem>(tasks)
            do! rt.ProcessMonitor.IncreaseTotalTasks(psInfo.Id, tasks.Length)
        }

    /// <summary>
    ///     Schedules a cloud workflow as a distributed result cell.
    ///     Used for child tasks.
    /// </summary>
    /// <param name="dependencies">Declared workflow dependencies.</param>
    /// <param name="cts">Cancellation token source bound to task.</param>
    /// <param name="wf">Input workflow.</param>
    member rt.StartAsCell(psInfo : ProcessInfo, dependencies, cts, fp, wf : Cloud<'T>, taskType) = async {
        let! resultCell = rt.ResourceFactory.RequestResultCell<'T>(psInfo.DefaultDirectory)
        let setResult ctx r = 
            async {
                do! resultCell.SetResult r
                TaskExecutionMonitor.TriggerCompletion ctx
            } |> TaskExecutionMonitor.ProtectAsync ctx

        let scont ctx t = setResult ctx (Completed t)
        let econt ctx e = setResult ctx (Exception e)
        let ccont ctx c = setResult ctx (Cancelled c)
        do! rt.EnqueueTask(psInfo, dependencies, cts, fp, scont, econt, ccont, wf, taskType)
        return resultCell
    }

    /// <summary>
    ///     Schedules a cloud workflow as a distributed result cell.
    ///     Used for root-level workflows.
    /// </summary>
    /// <param name="dependencies">Declared workflow dependencies.</param>
    /// <param name="cts">Cancellation token source bound to task.</param>
    /// <param name="wf">Input workflow.</param>
    /// <param name="psInfo">ProcessInfo.</param>
    member rt.StartAsProcess(psInfo : ProcessInfo, dependencies, cts : DistributedCancellationTokenSource, fp, wf : Cloud<'T>) = async {
        let! resultCell = rt.ResourceFactory.RequestResultCell<'T>(psInfo.DefaultDirectory)

        let! _ = rt.ProcessMonitor
                   .CreateRecord(psInfo.Id, psInfo.Name, typeof<'T>, dependencies,
                                   string cts.Uri, 
                                   string resultCell.Uri)

        let setResult ctx r = 
            async {
                do! resultCell.SetResult r
                let pmon = ctx.Resources.Resolve<ProcessManager>()
                match r with
                | Completed _ 
                | Exception _ -> do! pmon.SetCompleted(psInfo.Id)
                | Cancelled _ -> do! pmon.SetKilled(psInfo.Id)
                TaskExecutionMonitor.TriggerCompletion ctx
            } |> TaskExecutionMonitor.ProtectAsync ctx

        let scont ctx t = setResult ctx (Completed t)
        let econt ctx e = setResult ctx (Exception e)
        let ccont ctx c = setResult ctx (Cancelled c)
        do! rt.EnqueueTask(psInfo, dependencies, cts, fp, scont, econt, ccont, wf, TaskType.Root)
        return resultCell
    }

    /// Attempt to dequeue a task from the runtime task queue
    member rt.TryDequeue () : Async<QueueMessage option> =
        rt.TaskQueue.TryDequeue()