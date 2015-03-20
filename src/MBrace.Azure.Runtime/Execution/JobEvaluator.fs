namespace MBrace.Azure.Runtime

open MBrace.Azure
open Nessos.FsPickler
open Nessos.Vagabond.AppDomainPool
open System
open MBrace.Azure.Runtime.Primitives
open System.Diagnostics
open MBrace.Continuation
open MBrace.Store
open MBrace.Runtime
open MBrace.Azure.Runtime.Utilities
open MBrace.Runtime.Vagabond
open MBrace.Runtime.Store
open Nessos.Vagabond

/// Default job configuration for use by JobEvaluator.
type internal JobEvaluatorConfiguration =
    { Store   : ICloudFileStore
      Channel : ICloudChannelProvider
      Atom    : ICloudAtomProvider }

/// Static configuration per AppDomain.
type internal StaticConfiguration = 
    { State          : RuntimeState
      Resources      : ResourceRegistry
      Cache          : IObjectCache }

and [<AutoSerializable(false)>] 
    internal JobEvaluator(config : Configuration, serviceId : string, customLogger) =

    static let mutable staticConfiguration = Unchecked.defaultof<StaticConfiguration>

    static let mkAppDomainInitializer (config : Configuration) (serviceId : string) (customLogger : ICloudLogger) =
        fun () -> 
            async {
                
                let! state, resources = Init.Initializer(config, serviceId, false, customLogger)
                
                let inMemoryCache = InMemoryCache.Create()

                staticConfiguration <-
                    { State          = state
                      Resources      = resources
                      Cache          = inMemoryCache }

                state.Logger.Logf "AppDomain Initialized"
            }
            |> Async.RunSync


    static let runJob (config : JobEvaluatorConfiguration) (job : Job) (deps : AssemblyId list) (faultCount : int)  =
        let provider = RuntimeProvider.FromJob staticConfiguration.State deps job
        let info = job.ProcessInfo
        let serializer = staticConfiguration.Resources.Resolve<ISerializer>()
        let resources = resource { 
            yield! staticConfiguration.Resources
            yield { FileStore = defaultArg info.FileStore config.Store ; DefaultDirectory = info.DefaultDirectory; Cache = Some staticConfiguration.Cache; Serializer = serializer }
            yield { AtomProvider = defaultArg info.AtomProvider config.Atom ; DefaultContainer = info.DefaultAtomContainer }
            yield { ChannelProvider = defaultArg info.ChannelProvider config.Channel; DefaultContainer = info.DefaultChannelContainer }
        }
        Job.RunAsync provider resources faultCount job

    static let run (config : JobEvaluatorConfiguration) (msg : QueueMessage) (jobItem : PickledJob) = 
        async {
            let inline logf fmt = Printf.ksprintf (staticConfiguration.State.Logger :> ICloudLogger).Log fmt
            if msg.DeliveryCount = 1 then
                do! staticConfiguration.State.ProcessManager.AddActiveJob(jobItem.ProcessInfo.Id)

            let! jobResult = Async.Catch <| async {
                do! staticConfiguration.State.AssemblyManager.LoadDependencies(jobItem.Dependencies)
                return jobItem.ToJob()
            }

            match jobResult with
            | Choice2Of2 ex ->
                logf "Failed to UnPickle Job :\n%A" ex
                logf "SetResultUnsafe ResultCell %A" jobItem.ResultCell
                let pk, rk = jobItem.ResultCell
                do! ResultCell<obj>.SetResultUnsafe(jobItem.ConfigurationId, pk, rk, ex)
                let parentTaskCTS = jobItem.CancellationTokenSource
                logf "Cancel CancellationTokenSource %O" parentTaskCTS
                parentTaskCTS.Cancel()
                if jobItem.JobType = JobType.Root then
                    logf "Setting process completed"
                    do! staticConfiguration.State.ProcessManager.SetCompleted(jobItem.ProcessInfo.Id)
                logf "Faulted message : Complete."
                do! staticConfiguration.State.ProcessManager.AddFaultedJob(jobItem.ProcessInfo.Id)
                do! staticConfiguration.State.JobQueue.CompleteAsync(msg)
            | Choice1Of2 job ->
                if job.JobType = JobType.Root then
                    logf "Starting Root job for Process Id : %s, Name : %s" job.ProcessInfo.Id job.ProcessInfo.Name
                    do! staticConfiguration.State.ProcessManager.SetRunning(job.ProcessInfo.Id)

                logf "Starting job\n%s" (string job)
                let sw = Stopwatch.StartNew()
                let! result = Async.Catch(runJob config job jobItem.Dependencies (msg.DeliveryCount-1))
                sw.Stop()

                match result with
                | Choice1Of2 () -> 
                    do! staticConfiguration.State.JobQueue.CompleteAsync(msg)
                    do! staticConfiguration.State.ProcessManager.AddCompletedJob(job.ProcessInfo.Id)
                    logf "Completed job\n%s\nTime : %O" (string job) sw.Elapsed
                | Choice2Of2 e -> 
                    do! staticConfiguration.State.JobQueue.AbandonAsync(msg)
                    do! staticConfiguration.State.ProcessManager.AddFaultedJob(job.ProcessInfo.Id)
                    logf "Job fault\n%s\nwith :\n%O" (string job) e
        }

    let pool = AppDomainEvaluatorPool.Create(
                mkAppDomainInitializer config serviceId customLogger, 
                threshold = TimeSpan.FromDays 2., 
                minimumConcurrentDomains = 4,
                maximumConcurrentDomains = 64)

    member __.EvaluateAsync(config : JobEvaluatorConfiguration, message : QueueMessage) = async {
        let! jobItem = message.GetPayloadAsync<PickledJob>()
        return! pool.EvaluateAsync(jobItem.Dependencies, Async.Catch(run config message jobItem))
    }
