namespace MBrace.Azure.Runtime

#nowarn "0444" // MBrace.Core warnings

open System
open System.Text
open System.Threading.Tasks

open Microsoft.FSharp.Core.Printf

open Nessos.FsPickler
open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Vagabond
open MBrace.Azure
open MBrace.Azure.Runtime.Info
open MBrace.Azure.Runtime.Primitives
open MBrace.Azure.Runtime.Utilities

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
        /// ConfigurationId.
        ConfigurationId : ConfigurationId
    }
with
    /// Initialize a new runtime state in the local process
    static member FromConfiguration (config : Configuration, ignoreVersionCompatibility) = async {
        let configurationId = config.ConfigurationId
        let logger = new LoggerCombiner()
        let! jobQueue = JobQueue.Create(configurationId, logger)
        if not ignoreVersionCompatibility then
            Metadata.compare { Version = ReleaseInfo.localVersion; ConfigurationId = configurationId } jobQueue.Metadata

        let assemblyManager = BlobAssemblyManager.Create(configurationId, logger) 
        let resourceFactory = ResourceFactory.Create(configurationId) 
        let pman = ProcessManager.Create(configurationId)
        let wman = WorkerManager.Create(configurationId, logger)
        return { 
            ConfigurationId = configurationId
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
    member internal this.EnqueueJobBatch(psInfo, dependencies, cts, fp, scFactory, ec, cc, wfs : (#Cloud<'T> * IWorkerRef option) [], distribType : DistributionType, parentJobId, resultCell) : Async<unit> =
        async {
            let jobs = Array.zeroCreate wfs.Length
            for i = 0 to wfs.Length - 1 do
                let jobId = guid()
                let wf = fst wfs.[i]
                let affinity = match snd wfs.[i] with Some wr -> Some wr.Id | None -> None
                let stathisJob ctx =
                    let cont = { Success = scFactory i ; Exception = ec ; Cancellation = cc }
                    Cloud.StartWithContinuations(wf, cont, ctx)
                let jobType aff =
                    match distribType, aff with
                    | Parallel, Some a -> ParallelAffined(a, i, wfs.Length-1)
                    | Choice, Some a   -> ChoiceAffined(a, i, wfs.Length-1)
                    | Parallel, None   -> JobType.Parallel(i,wfs.Length-1)
                    | Choice, None     -> JobType.Choice(i,wfs.Length-1)

                let pickle value = VagabondRegistry.Instance.Pickler.PickleTyped(value)

                let job = 
                    { 
                        ConfigurationId         = this.ConfigurationId
                        PickledType             = pickle typeof<'T>
                        ProcessInfo             = psInfo
                        JobId                   = jobId
                        PickledStartJob         = pickle stathisJob
                        CancellationTokenSource = cts
                        PickledFaultPolicy      = pickle fp
                        PickledEcont            = pickle ec
                        JobType                 = jobType affinity
                        ParentJobId             = parentJobId
                        Dependencies            = dependencies
                        ResultCell              = resultCell
                    }
                jobs.[i] <- job, affinity
            do! this.JobQueue.EnqueueBatch<PickledJob>(jobs, pid = psInfo.Id)
            do! this.ProcessManager.IncreaseTotalJobs(psInfo.Id, jobs.Length)
        }

    member private this.EnqueueJob(psInfo, jobId, dependencies, cts, fp, sc, ec, cc, wf : Cloud<'T>, jobType : JobType, parentJobId, resultCell) : Async<unit> =
        async {
            let startJob ctx =
                let cont = { Success = sc ; Exception = ec ; Cancellation = cc }
                Cloud.StartWithContinuations(wf, cont, ctx)
            let affinity = match jobType with Affined a -> Some a | _ -> None
            let pickle value = VagabondRegistry.Instance.Pickler.PickleTyped(value)

            let job = 
                { 
                    ConfigurationId         = this.ConfigurationId
                    PickledType             = pickle typeof<'T>
                    ProcessInfo             = psInfo
                    JobId                   = jobId
                    PickledStartJob         = pickle startJob
                    CancellationTokenSource = cts
                    PickledFaultPolicy      = pickle fp
                    PickledEcont            = pickle ec
                    JobType                 = jobType 
                    ParentJobId             = parentJobId
                    Dependencies            = dependencies
                    ResultCell              = resultCell
                }

            this.Logger.Logf "Job Enqueue."
            do! this.JobQueue.Enqueue<PickledJob>(job, ?affinity = affinity, pid = psInfo.Id)
            this.Logger.Logf "Job Enqueue completed."
            do! this.ProcessManager.IncreaseTotalJobs(psInfo.Id)
        }

    /// Schedules a cloud workflow as an ICloudTask.
    member internal this.StartAsTask(psInfo : ProcessInfo, dependencies, ct:ICloudCancellationToken option, fp, wf : Cloud<'T>, jobType, parentJobId) : Async<ICloudTask<'T>> = async {
        let jobId = guid()
        let! resultCell = async {
            let batch = this.ResourceFactory.GetResourceBatchForProcess(psInfo.Id)
            let rc = batch.RequestResultCell(jobId)
            do! batch.CommitAsync()
            return rc
        }

        // create a new cancellation token source if none specified, or a child cancellation token if supplied
        // this will allow cancellation once task has been completed.
        let parentCts = ct |> Option.map unbox<DistributedCancellationTokenSource>
        let! childCts = this.ResourceFactory.RequestCancellationTokenSource(jobId, ?parent = parentCts, metadata = jobId, elevate = true)

        let setResult ctx r = 
            async {
                let! result = Async.Catch <| resultCell.SetResult r
                match result with
                | Choice1Of2 () -> ()
                | Choice2Of2 ex when (ex :? Nessos.FsPickler.NonSerializableTypeException) ->
                    let ex = new FaultException(sprintf "Failed to execute job '%s'" jobId ,ex)
                    do! resultCell.SetResultUnsafe(ex)
                | _ -> ()
                childCts.Cancel()
                JobExecutionMonitor.TriggerCompletion ctx
            } |> JobExecutionMonitor.ProtectAsync ctx
            
        let scont ctx t = setResult ctx (Result.Completed t)
        let econt ctx e = setResult ctx (Result.Exception e)
        let ccont ctx c = setResult ctx (Result.Cancelled c)
        do! this.EnqueueJob(psInfo, jobId, dependencies, childCts, fp, scont, econt, ccont, wf, jobType, parentJobId, (resultCell.PartitionKey, resultCell.RowKey))
        return resultCell :> ICloudTask<'T>
    }

    /// Schedules a cloud workflow as an ICloudJob.
    /// Used for root-level workflows.
    member this.StartAsProcess(psInfo : ProcessInfo, dependencies, fp, wf : Cloud<'T>, ?ct : ICloudCancellationToken) = async {
        let jobId = guid ()
        
        this.Logger.Logf "Request for CancellationTokenSource"
        let! cts = 
            match ct with
            | None -> this.ResourceFactory.RequestCancellationTokenSource(psInfo.Id, metadata = jobId, elevate = true)
            | Some ct -> async { return ct :?> DistributedCancellationTokenSource }
        
        let requests = this.ResourceFactory.GetResourceBatchForProcess(psInfo.Id)
        let resultCell = requests.RequestResultCell<'T>(jobId)
        this.Logger.Logf "Creating Process Record for %s" psInfo.Id
        do! Async.Parallel [| this.ProcessManager.CreateRecord(psInfo.Id, psInfo.Name, typeof<'T>, dependencies, cts, resultCell.RowKey)
                              requests.CommitAsync() |]
            |> Async.Ignore

        let setResult ctx r = 
            async {
                let! result = Async.Catch <| resultCell.SetResult r
                let pmon = ctx.Resources.Resolve<ProcessManager>()
                match result with
                | Choice1Of2 () ->
                    match r with
                    | Result.Completed _ 
                    | Result.Exception _ -> do! pmon.SetCompleted(psInfo.Id)
                    | Result.Cancelled _ -> do! pmon.SetCancelled(psInfo.Id)
                | Choice2Of2 ex when (ex :? Nessos.FsPickler.NonSerializableTypeException) ->
                    let ex = new FaultException(sprintf "Failed to execute job '%s'" jobId ,ex)
                    do! resultCell.SetResultUnsafe(ex)
                    do! pmon.SetFaulted(psInfo.Id)
                | _ -> ()
                JobExecutionMonitor.TriggerCompletion ctx
            } |> JobExecutionMonitor.ProtectAsync ctx

        let scont ctx t = setResult ctx (Result.Completed t)
        let econt ctx e = setResult ctx (Result.Exception e)
        let ccont ctx c = setResult ctx (Result.Cancelled c)

        try
            do! this.EnqueueJob(psInfo, jobId, dependencies, cts, fp, scont, econt, ccont, wf, JobType.Root, String.Empty, (resultCell.PartitionKey, resultCell.RowKey))
            return resultCell
        with ex ->
            this.Logger.Logf "Failed to post process %s. Cleanup." psInfo.Id
            do! this.ProcessManager.ClearProcess(psInfo.Id, true, true)
            return! Async.Raise ex
    }

    /// Attempt to dequeue a job from the runtime job queue.
    member this.TryDequeue () : Async<QueueMessage option> =
        async { return! this.JobQueue.TryDequeue() }