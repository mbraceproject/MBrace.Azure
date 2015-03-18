namespace MBrace.Azure.Runtime

// Provides facility for the execution of jobs.
// In this context, a job denotes a single work item to be sent
// to a worker node for execution. Jobs may span multiple threads
// but are bound to a single process. A cloud workflow that has
// been passed continuations is a typical example of such a job.

#nowarn "0444" // MBrace.Core warnings

open System
open System.Threading.Tasks

open Nessos.FsPickler
open Nessos.Vagabond

open MBrace
open MBrace.Azure
open MBrace.Azure.Runtime.Primitives
open MBrace.Azure.Runtime.Info
open MBrace.Azure.Runtime.Utilities
open MBrace.Continuation
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Vagabond
open MBrace.Store
open Microsoft.FSharp.Core.Printf
open System.Text

// Jobs are cloud workflows that have been attached to continuations.
// In that sense they are 'closed' multi-threaded computations that
// are difficult to reason about from a worker node's point of view.
// JobExecutionMonitor provides a way to cooperatively track execution
// of such 'closed' computations.

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

/// Job kind.
type JobType =
    /// Root job for process.
    | Root
    /// Job created by Cloud.StartChild.
    | StartChild
    /// Job created by Cloud.StartChild with affinity.
    | Affined of affinity : string
    /// Job created by Cloud.Parallel.
    | Parallel of index : int * maxIndex : int
    /// Job created by Cloud.Choice.
    | Choice of index : int * maxIndex : int
    /// Job created by Cloud.Parallel with affinity.
    | ParallelAffined of affinity : string * index : int * maxIndex : int
    /// Job created by Cloud.Choice with affinity.
    | ChoiceAffined of affinity : string * index : int * maxIndex : int

type internal DistributionType =
    | Choice
    | Parallel

/// Defines a job to be executed in a worker node
type Job = 
    {
        /// Return type of the defining cloud workflow.
        Type : Type
        /// Process information record.
        ProcessInfo : ProcessInfo
        /// Job unique identifier.
        JobId : string
        /// Triggers job execution with worker-provided execution context.
        StartJob : ExecutionContext -> unit
        /// Job fault policy.
        FaultPolicy : FaultPolicy
        /// Exception Continuation.
        Econt : ExecutionContext -> ExceptionDispatchInfo -> unit
        /// Distributed cancellation token source bound to job.
        CancellationTokenSource : DistributedCancellationTokenSource
        /// Type of job.
        JobType : JobType
        /// JobId of parent Job.
        ParentJobId : string
    }
with
    override this.ToString () =
        let sb = StringBuilder()
        bprintf sb "Job : %s\n" this.JobId
        bprintf sb "ParentJob : %s\n" (if String.IsNullOrEmpty this.ParentJobId then "<empty>" else this.ParentJobId)
        bprintf sb "ProcessId : %s\n" this.ProcessInfo.Id
        bprintf sb "ReturnType : %s\n" (Runtime.Utils.PrettyPrinters.Type.prettyPrint this.Type)
        bprintf sb "JobType : %A" this.JobType
        sb.ToString()

    /// <summary>
    ///     Asynchronously executes job in the local process.
    /// </summary>
    /// <param name="runtimeProvider">Local scheduler implementation.</param>
    /// <param name="dependencies">Job dependent assemblies.</param>
    /// <param name="job">Job to be executed.</param>
    static member RunAsync (runtimeProvider : IDistributionProvider) 
                           (resources : ResourceRegistry)
                           (faultCount : int)
                           (job : Job) = async {
        let jem = new JobExecutionMonitor()
        let ctx =
            {
                Resources = resource { 
                                yield runtimeProvider
                                yield! resources
                                yield jem
                                yield job.CancellationTokenSource
                            }
                CancellationToken = job.CancellationTokenSource :> ICloudCancellationToken
            }

        if faultCount > 0 then
            let faultException = new FaultException(sprintf "Fault exception when running job '%s', faultCount '%d'" job.JobId faultCount)
            match job.FaultPolicy.Policy faultCount (faultException :> exn) with
            | None -> 
                job.Econt ctx <| ExceptionDispatchInfo.Capture faultException
            | Some timeout ->
                do! Async.Sleep (int timeout.TotalMilliseconds)
                do job.StartJob ctx
        else
            do job.StartJob ctx

        return! JobExecutionMonitor.AwaitCompletion jem
    }

/// JobQueue message type.
type JobItem = 
    { PickledJob : Pickle<Job>
      Dependencies : AssemblyId list }

[<AutoSerializable(false)>]
/// Defines a handle to the state of a runtime instance.
type RuntimeState =
    {
        /// Reference to the global job queue employed by the runtime
        /// Queue contains pickled job and its dependencies.
        JobQueue : JobQueue
        /// Assembly manager.
        AssemblyManager : BlobAssemblyManager
        /// Reference to the runtime resource manager
        /// Used for generating latches, cancellation tokens and result cells.
        ResourceFactory : ResourceFactory
        /// Process management.
        ProcessManager : ProcessManager
        /// Worker management.
        WorkerManager : WorkerManager
        /// Runtime Logger.
        Logger : LoggerCombiner
    }
with
    /// Initialize a new runtime state in the local process
    static member FromConfiguration (config : Configuration) = async {
        let configurationId = config.ConfigurationId
        let logger = new LoggerCombiner()
        let! jobQueue = JobQueue.Create(configurationId, logger)
        let assemblyManager = BlobAssemblyManager.Create(configurationId, logger) 
        let resourceFactory = ResourceFactory.Create(configurationId) 
        let pman = ProcessManager.Create(configurationId)
        let wman = WorkerManager.Create(configurationId, logger)
        return { 
            JobQueue = jobQueue
            AssemblyManager = assemblyManager 
            ResourceFactory = resourceFactory 
            ProcessManager = pman
            WorkerManager = wman
            Logger = logger
        }
    }

    /// <summary>
    ///     Enqueue a batch of cloud workflows with supplied continuations to the runtime job queue.
    ///     Used for Parallel and Choice combinators
    /// </summary>
    /// <param name="dependencies">Vagrant dependency manifest.</param>
    /// <param name="cts">Distributed cancellation token source.</param>
    /// <param name="scFactory">Success continuation factory.</param>
    /// <param name="ec">Exception continuation.</param>
    /// <param name="cc">Cancellation continuation.</param>
    /// <param name="wfs">Workflows</param>
    /// <param name="affinity">Optional job affinity.</param>
    member internal rt.EnqueueJobBatch(psInfo, dependencies, cts, fp, scFactory, ec, cc, wfs : (#Cloud<'T> * IWorkerRef option) [], distribType : DistributionType, parentJobId) : Async<unit> =
        async {
            let jobs = Array.zeroCreate wfs.Length
            for i = 0 to wfs.Length - 1 do
                let jobId = guid()
                let wf = fst wfs.[i]
                let affinity = match snd wfs.[i] with Some wr -> Some wr.Id | None -> None
                let startJob ctx =
                    let cont = { Success = scFactory i ; Exception = ec ; Cancellation = cc }
                    Cloud.StartWithContinuations(wf, cont, ctx)
                let jobType aff  =
                    match distribType, aff with
                    | Parallel, Some a -> ParallelAffined(a, i, wfs.Length-1)
                    | Choice, Some a   -> ChoiceAffined(a, i, wfs.Length-1)
                    | Parallel, None   -> JobType.Parallel(i,wfs.Length-1)
                    | Choice, None     -> JobType.Choice(i,wfs.Length-1)

                let job = 
                    { 
                        Type = typeof<'T>
                        ProcessInfo = psInfo
                        JobId = jobId
                        StartJob = startJob
                        CancellationTokenSource = cts
                        FaultPolicy = fp
                        Econt = ec
                        JobType = jobType affinity
                        ParentJobId = parentJobId
                    }

                let jobp = VagabondRegistry.Instance.Pickler.PickleTyped job
                jobs.[i] <- { PickledJob = jobp; Dependencies = dependencies }, affinity
            do! rt.JobQueue.EnqueueBatch<JobItem>(jobs, pid = psInfo.Id)
            do! rt.ProcessManager.IncreaseTotalJobs(psInfo.Id, jobs.Length)
        }

    member private rt.EnqueueJob(psInfo, jobId, dependencies, cts, fp, sc, ec, cc, wf : Cloud<'T>, jobType : JobType, parentJobId, ?logger : ICloudLogger) : Async<unit> =
        async {
            let logger = defaultArg logger (NullLogger() :> _)
        
            let startJob ctx =
                let cont = { Success = sc ; Exception = ec ; Cancellation = cc }
                Cloud.StartWithContinuations(wf, cont, ctx)
            let affinity = match jobType with Affined a -> Some a | _ -> None
            let job = 
                { 
                    Type = typeof<'T>
                    ProcessInfo = psInfo
                    JobId = jobId
                    StartJob = startJob
                    CancellationTokenSource = cts
                    FaultPolicy = fp
                    Econt = ec
                    JobType = jobType
                    ParentJobId = parentJobId
                }
        
            logger.Logf "Pickle Job."
            let jobp = VagabondRegistry.Instance.Pickler.PickleTyped job
            logger.Logf "Pickled Job [%d bytes]." jobp.Bytes.Length
            let jobItem = { PickledJob = jobp; Dependencies = dependencies }
            logger.Logf "Job Enqueue."
            do! rt.JobQueue.Enqueue<JobItem>(jobItem, ?affinity = affinity, pid = psInfo.Id)
            logger.Logf "Job Enqueue completed."
            do! rt.ProcessManager.IncreaseTotalJobs(psInfo.Id)
        }

    /// Schedules a cloud workflow as an ICloudTask.
    member internal rt.StartAsTask(psInfo : ProcessInfo, dependencies, cts, fp, wf : Cloud<'T>, jobType, parentJobId) : Async<ICloudTask<'T>> = async {
        let jobId = guid()
        let! resultCell = async {
            let batch = rt.ResourceFactory.GetResourceBatchForProcess(psInfo.Id)
            let rc = batch.RequestResultCell(jobId)
            do! batch.CommitAsync()
            return rc
        }
        let setResult ctx r = 
            async {
                do! resultCell.SetResult r
                JobExecutionMonitor.TriggerCompletion ctx
            } |> JobExecutionMonitor.ProtectAsync ctx
            
        let scont ctx t = setResult ctx (Result.Completed t)
        let econt ctx e = setResult ctx (Result.Exception e)
        let ccont ctx c = setResult ctx (Result.Cancelled c)
        do! rt.EnqueueJob(psInfo, jobId, dependencies, cts, fp, scont, econt, ccont, wf, jobType, parentJobId)
        return resultCell :> ICloudTask<'T>
    }

    /// Schedules a cloud workflow as an ICloudJob.
    /// Used for root-level workflows.
    member rt.StartAsProcess(psInfo : ProcessInfo, dependencies, fp, wf : Cloud<'T>, logger : ICloudLogger, ?ct : ICloudCancellationToken) = async {
        let jobId = guid ()
        
        logger.Logf "Request for CancellationTokenSource"
        let! cts = 
            match ct with
            | None -> rt.ResourceFactory.RequestCancellationTokenSource(psInfo.Id, metadata = jobId, elevate = true)
            | Some ct -> async { return ct :?> DistributedCancellationTokenSource }
        
        let requests = rt.ResourceFactory.GetResourceBatchForProcess(psInfo.Id)
        let resultCell = requests.RequestResultCell<'T>(jobId)
        logger.Logf "Creating Process Record for %s" psInfo.Id
        do! Async.Parallel [| rt.ProcessManager.CreateRecord(psInfo.Id, psInfo.Name, typeof<'T>, dependencies, cts, string resultCell.Path)
                              requests.CommitAsync() |]
            |> Async.Ignore

        let setResult ctx r = 
            async {
                do! resultCell.SetResult r
                let pmon = ctx.Resources.Resolve<ProcessManager>()
                match r with
                | Result.Completed _ 
                | Result.Exception _ -> do! pmon.SetCompleted(psInfo.Id)
                | Result.Cancelled _ -> do! pmon.SetKilled(psInfo.Id)
                JobExecutionMonitor.TriggerCompletion ctx
            } |> JobExecutionMonitor.ProtectAsync ctx

        let scont ctx t = setResult ctx (Result.Completed t)
        let econt ctx e = setResult ctx (Result.Exception e)
        let ccont ctx c = setResult ctx (Result.Cancelled c)

        do! rt.EnqueueJob(psInfo, jobId, dependencies, cts, fp, scont, econt, ccont, wf, JobType.Root, String.Empty, logger = logger)
        return resultCell
    }

    /// Attempt to dequeue a job from the runtime job queue.
    member rt.TryDequeue () : Async<QueueMessage option> =
        async { return! rt.JobQueue.TryDequeue() }