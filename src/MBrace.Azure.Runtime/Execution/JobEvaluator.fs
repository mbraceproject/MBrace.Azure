namespace MBrace.Azure.Runtime

open System
open System.Diagnostics

open Nessos.FsPickler
open Nessos.Vagabond
open Nessos.Vagabond.AppDomainPool

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals
open MBrace.Runtime.Vagabond
open MBrace.Runtime.Store
open MBrace.Azure
open MBrace.Azure.Runtime.Primitives
open MBrace.Azure.Runtime.Utilities

/// Default job configuration for use by JobEvaluator.
type internal JobEvaluatorConfiguration =
    { Store           : ICloudFileStore
      StoreDirectory  : string option
      Channel         : ICloudChannelProvider
      ChannelDirectory: string option
      Atom            : ICloudAtomProvider
      AtomDirectory   : string option
      CustomResources : ResourceRegistry }

/// Static configuration per AppDomain.
type internal StaticConfiguration = 
    { State          : RuntimeState
      Resources      : ResourceRegistry
      Cache          : IObjectCache }

and [<AutoSerializable(false)>] 
    internal JobEvaluator(config : Configuration, serviceId : string, customLogger, ignoreVersion : bool) =

    static let mutable staticConfiguration = Unchecked.defaultof<StaticConfiguration>

    static let mkAppDomainInitializer (config : Configuration) (serviceId : string) (customLogger : ICloudLogger) (ignoreVersion : bool) =
        fun () -> 
            async {
                
                let! state, resources = Init.Initializer(config, serviceId, false, customLogger, ignoreVersion)
                
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
        let getDirectory (client : string option) (worker : string option) (configDefault : string) : string =
            match client, worker, configDefault with
            | Some c, _, _
            | None, Some c, _
            | None, None, c -> c

        let resources = resource { 
            yield! staticConfiguration.Resources
            yield! config.CustomResources
            yield staticConfiguration.Cache
            yield serializer
            yield { FileStore = defaultArg info.FileStore config.Store
                    DefaultDirectory = getDirectory info.DefaultDirectory config.StoreDirectory staticConfiguration.State.ConfigurationId.UserDataContainer }
            yield { AtomProvider = defaultArg info.AtomProvider config.Atom
                    DefaultContainer = getDirectory info.DefaultAtomContainer config.AtomDirectory staticConfiguration.State.ConfigurationId.UserDataTable }
            yield { ChannelProvider = defaultArg info.ChannelProvider config.Channel
                    DefaultContainer = getDirectory info.DefaultChannelContainer config.ChannelDirectory staticConfiguration.State.ConfigurationId.UserDataTable }
        }
        Job.RunAsync provider resources faultCount job

    static let run (config : JobEvaluatorConfiguration) (msg : QueueMessage) (dependencies : VagabondAssembly list) (jobItem : PickledJob) = 
        async {
            let inline logf fmt = Printf.ksprintf (staticConfiguration.State.Logger :> ICloudLogger).Log fmt

            if msg.DeliveryCount = 1 then
                do! staticConfiguration.State.ProcessManager.AddActiveJob(jobItem.ProcessInfo.Id)

            logf "Loading assemblies"
            let! jobResult = Async.Catch <| async {
                let _ = staticConfiguration.State.AssemblyManager.LoadAssemblies dependencies
                logf "UnPickle Job"
                return jobItem.ToJob()
            }

            match jobResult with
            | Choice2Of2 ex ->
                logf "Failed to UnPickle Job :\n%A" ex
                return! FaultHandler.FaultPickledJobAsync(jobItem, msg, staticConfiguration.State, ex)
            | Choice1Of2 job ->
                if job.JobType = JobType.Root then
                    logf "Starting Root job for Process Id : %s, Name : %s" job.ProcessInfo.Id job.ProcessInfo.Name
                    do! staticConfiguration.State.ProcessManager.SetRunning(job.ProcessInfo.Id)

                logf "Starting job\n%s" (string job)
                logf "Delivery count : %d" msg.DeliveryCount
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
                mkAppDomainInitializer config serviceId customLogger ignoreVersion, 
                threshold = TimeSpan.FromDays 2., 
                minimumConcurrentDomains = 4,
                maximumConcurrentDomains = 64)

    member __.EvaluateAsync(config : JobEvaluatorConfiguration, dependencies : VagabondAssembly list, message : QueueMessage, job : PickledJob) = async {
        return! pool.EvaluateAsync(job.Dependencies, Async.Catch(run config message dependencies job))
    }
