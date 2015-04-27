namespace MBrace.Azure.Runtime

// Provides facility for the execution of jobs.
// In this context, a job denotes a single work item to be sent
// to a worker node for execution. Jobs may span multiple threads
// but are bound to a single process. A cloud workflow that has
// been passed continuations is a typical example of such a job.

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
open MBrace.Store.Internals
open MBrace.Runtime.Utils
open MBrace.Runtime.Vagabond
open MBrace.Azure
open MBrace.Azure.Runtime.Info
open MBrace.Azure.Runtime.Primitives
open MBrace.Azure.Runtime.Utilities

// Jobs are cloud workflows that have been attached to continuations.
// In that sense they are 'closed' multi-threaded computations that
// are difficult to reason about from a worker node's point of view.
// JobExecutionMonitor provides a way to cooperatively track execution
// of such 'closed' computations.

/// Process information record.
type ProcessInfo =
    {
        /// Cloud process unique identifier.
        Id : string
        /// Process name.
        Name : string

        /// Default file store container for process.
        DefaultDirectory : string option
        /// Default atom container for process.
        DefaultAtomContainer : string option
        /// Default channel container for process.
        DefaultChannelContainer : string option

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
        /// Parent task result cell path
        ResultCell : string * string
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
        bprintf sb "ReturnType : %s\n" (PrettyPrinters.Type.prettyPrint this.Type)
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
        let ctx, jem = Job.CreateExecutionContext runtimeProvider resources job
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
        
        let! result =  Async.Catch <| JobExecutionMonitor.AwaitCompletion jem
        match result with
        | Choice1Of2 () -> 
            return true
        | Choice2Of2 ex ->
            let fault = new FaultException(sprintf "Failed to execute job '%s'" job.JobId, ex)
            match job.FaultPolicy.Policy (faultCount + 1) (fault :> exn) with
            | None ->
                job.Econt ctx <| ExceptionDispatchInfo.Capture fault
                return false
            | _ ->
                return! Async.Raise ex
    }

    static member CreateExecutionContext (runtimeProvider : IDistributionProvider) (resources : ResourceRegistry) (job : Job) =
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
        ctx, jem



/// JobQueue message type.
type PickledJob = 
    {
        /// Process information record.
        ProcessInfo : ProcessInfo
        /// Job unique identifier.
        JobId : string
        /// Distributed cancellation token source bound to job.
        CancellationTokenSource : DistributedCancellationTokenSource
        /// Type of job.
        JobType : JobType
        /// JobId of parent Job.
        ParentJobId : string
        /// Dependency manifest.
        Dependencies : AssemblyId list 
        /// Parent task result cell path
        ResultCell : string * string

        /// Job ConfigurationId.
        ConfigurationId : ConfigurationId

        /// Return type of the defining cloud workflow.
        PickledType : Pickle<Type>
        /// Triggers job execution with worker-provided execution context.
        PickledStartJob : Pickle<ExecutionContext -> unit>
        /// Job fault policy.
        PickledFaultPolicy : Pickle<FaultPolicy>
        /// Exception Continuation.
        PickledEcont : Pickle<ExecutionContext -> ExceptionDispatchInfo -> unit>
    }
    
    member this.ToJob() : Job =
        let unpickle pickle = VagabondRegistry.Instance.Pickler.UnPickleTyped(pickle)
        {
            ProcessInfo = this.ProcessInfo
            Type = unpickle this.PickledType
            JobId = this.JobId
            StartJob = unpickle this.PickledStartJob
            FaultPolicy = unpickle this.PickledFaultPolicy
            Econt = unpickle this.PickledEcont
            CancellationTokenSource = this.CancellationTokenSource
            JobType = this.JobType
            ParentJobId = this.ParentJobId
            ResultCell = this.ResultCell
        }
