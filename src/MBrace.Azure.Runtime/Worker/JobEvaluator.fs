namespace MBrace.Azure.Runtime

open MBrace.Azure
open Nessos.FsPickler
open Nessos.Vagabond.AppDomainPool
open System
open MBrace.Azure.Runtime.Resources
open System.Diagnostics
open MBrace.Continuation
open MBrace.Store
open MBrace.Runtime
open MBrace.Azure.Runtime.Common
open MBrace.Runtime.Vagabond
open MBrace.Runtime.Store
open Nessos.Vagabond

type internal JobEvaluatorConfiguration =
    { Store   : ICloudFileStore
      Channel : ICloudChannelProvider
      Atom    : ICloudAtomProvider }

type internal StaticJobEvaluatorConfiguration = 
    { State          : RuntimeState
      Resources      : ResourceRegistry
      Cache          : IObjectCache
      WorkerManager  : WorkerManager
      ProcessManager : ProcessManager }

type internal WorkerConfig = 
    { State                     : RuntimeState
      MaxConcurrentJobs         : int
      Resources                 : ResourceRegistry
      JobEvaluatorConfiguration : JobEvaluatorConfiguration
      Logger                    : ICloudLogger
      WorkerManager             : WorkerManager
      ProcessManager            : ProcessManager }

[<AutoSerializable(false)>]
type internal JobEvaluator(config : Configuration, serviceId : string) =

    static let mutable staticConfiguration = Unchecked.defaultof<StaticJobEvaluatorConfiguration>

    static let mkAppDomainInitializer (config : Configuration) (serviceId : string) =
        fun () -> 
            async {
                let config = config.WithAppendedId
                do! Configuration.ActivateAsync(config)

                let logger = new StorageLogger(config.ConfigurationId, Worker(id = serviceId))
                
                let serializer = Configuration.Serializer
                let! state = RuntimeState.FromConfiguration(config)
                state.Logger.ShowAppDomainAsPrefix <- true
                state.Logger.Attach(logger)
                state.Logger.Attach(new MBrace.Azure.ConsoleLogger())
                state.JobQueue.Affinity <- serviceId
                let inMemoryCache = InMemoryCache.Create()
                let workerManager = WorkerManager.Create(config.ConfigurationId, state.Logger)
                do! workerManager.RegisterLocal(serviceId)
                let resources = resource { 
                    yield serializer
                    yield logger
                    yield workerManager
                    yield state.ProcessManager 
                }

                staticConfiguration <-
                    { State = state
                      Resources = resources
                      Cache = inMemoryCache
                      WorkerManager = workerManager
                      ProcessManager = state.ProcessManager
                    }

                state.Logger.Logf "AppDomain Initialized"
            }
            |> Async.RunSynchronously


    static let runJob (config : JobEvaluatorConfiguration) (job : Job) (deps : AssemblyId list) (faultCount : int)  =
        let provider = RuntimeProvider.FromJob staticConfiguration.State staticConfiguration.WorkerManager deps job
        let info = job.ProcessInfo
        let serializer = staticConfiguration.Resources.Resolve<ISerializer>()
        let resources = resource { 
            yield! staticConfiguration.Resources
            yield { FileStore = defaultArg info.FileStore config.Store ; DefaultDirectory = info.DefaultDirectory; Cache = Some staticConfiguration.Cache; Serializer = serializer }
            yield { AtomProvider = defaultArg info.AtomProvider config.Atom ; DefaultContainer = info.DefaultAtomContainer }
            yield { ChannelProvider = defaultArg info.ChannelProvider config.Channel; DefaultContainer = info.DefaultChannelContainer }
        }
        Job.RunAsync provider resources faultCount job

    static let run (config : JobEvaluatorConfiguration) (msg : QueueMessage) (jobItem : JobItem) = async {
        let inline logf fmt = Printf.ksprintf (staticConfiguration.State.Logger :> ICloudLogger).Log fmt
        try
            logf "Loading dependencies"
            do! staticConfiguration.State.AssemblyManager.LoadDependencies(jobItem.Dependencies)
            
            logf "UnPickle Job [%d bytes]" jobItem.PickledJob.Bytes.Length
            let job = VagabondRegistry.Instance.Pickler.UnPickleTyped(jobItem.PickledJob)

            if job.JobType = JobType.Root then
                logf "Starting Root job for Process Id : %s, Name : %s" job.ProcessInfo.Id job.ProcessInfo.Name
                do! staticConfiguration.ProcessManager.SetRunning(job.ProcessInfo.Id)

            if msg.DeliveryCount = 1 then
                do! staticConfiguration.ProcessManager.AddActiveJob(job.ProcessInfo.Id)
            
            logf "Starting job\n%s" (string job)
            let sw = Stopwatch.StartNew()
            let! result = Async.Catch(runJob config job jobItem.Dependencies (msg.DeliveryCount-1))
            sw.Stop()

            match result with
            | Choice1Of2 () -> 
                do! staticConfiguration.State.JobQueue.CompleteAsync(msg)
                do! staticConfiguration.ProcessManager.AddCompletedJob(job.ProcessInfo.Id)
                logf "Completed job\n%s\nTime : %O" (string job) sw.Elapsed
            | Choice2Of2 e -> 
                do! staticConfiguration.State.JobQueue.AbandonAsync(msg)
                do! staticConfiguration.ProcessManager.AddFaultedJob(job.ProcessInfo.Id)
                logf "Job fault %s with :\n%O" (string job) e
        with ex ->
            logf "Failed to UnPickle Job :\n%A" ex
            if msg.DeliveryCount >= 1 then
                // TODO : Set Process as Faulted.
                logf "Faulted message : Complete."
                do! staticConfiguration.State.JobQueue.CompleteAsync(msg)
            else
                logf "Faulted message : Abandon."
                do! staticConfiguration.State.JobQueue.AbandonAsync(msg)
    }

    let pool = AppDomainEvaluatorPool.Create(
                mkAppDomainInitializer config serviceId, 
                threshold = TimeSpan.FromHours 2., 
                minimumConcurrentDomains = 1,
                maximumConcurrentDomains = 64)

    member __.EvaluateAsync(config : JobEvaluatorConfiguration, message : QueueMessage) = async {
        let! jobItem = message.GetPayloadAsync<JobItem>()
        return! pool.EvaluateAsync(jobItem.Dependencies, Async.Catch(run config message jobItem))
    }
