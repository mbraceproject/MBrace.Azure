namespace Nessos.MBrace.Azure.Runtime

open System
open System.Threading
open System.Threading.Tasks
open Nessos.MBrace.Azure.Runtime
open System.Runtime.InteropServices
open Nessos.MBrace.Azure.Runtime.Common
open Nessos.MBrace
open Nessos.MBrace.Runtime
open Nessos.MBrace.Continuation
open Nessos.MBrace.Store
open Nessos.MBrace.Azure.Runtime.Resources

/// MBrace Runtime Service.
type Service (config : Configuration, maxTasks : int, storeConfig : CloudFileStoreConfiguration, serviceId : string) =
    do Configuration.Activate(config)
    let state = RuntimeState.FromConfiguration(config)
    let logf fmt = Printf.ksprintf state.ResourceFactory.Logger.Log fmt

    /// MBrace Runtime Service.
    new(config : Configuration, maxTasks : int, store : CloudFileStoreConfiguration) = new Service (config, maxTasks, store, guid())

    member __.Configuration = config
    member __.Id = serviceId
    member __.AttachLogger(logger) = state.ResourceFactory.Logger.Attach(logger)
    member __.MaxConcurrentTasks = maxTasks
    member __.CloudFileStore = storeConfig
    member __.StartAsTask() : Tasks.Task = Async.StartAsTask(__.StartAsync()) :> _
    member __.StartAsTask(ct : CancellationToken) : Tasks.Task = Async.StartAsTask(__.StartAsync(), cancellationToken = ct) :> _
        
    member __.StartAsync() : Async<unit> =
        async {
            try
                logf "Starting Service %s" serviceId

                let subscription = state.TaskQueue.Affinity <- serviceId
                logf "Subscription for %s created" serviceId

                let atomConfig = { AtomProvider = AtomProvider.Create(config.ConfigurationId); DefaultContainer = config.DefaultTableOrContainer }
                logf "AtomProvider : %s" atomConfig.AtomProvider.Id

                let channelConfig = { ChannelProvider = ChannelProvider.Create(config.ConfigurationId); DefaultContainer = String.Empty }
                logf "ChannelProvider : %s" channelConfig.ChannelProvider.Id

                logf "CloudFileStore : %s" storeConfig.FileStore.Id

                let! (e : WorkerRef) = state.ResourceFactory.WorkerMonitor.DeclareCurrent(serviceId)
                logf "Declared node %s : %d : %s" e.Hostname e.ProcessId (e :> IWorkerRef).Id
                
                Async.Start(state.ResourceFactory.WorkerMonitor.HeartbeatLoop())
                logf "Started heartbeat loop" 

                logf "Starting worker loop"
                logf "MaxTasks : %d" maxTasks
                let resources = resource { yield storeConfig; yield atomConfig; yield channelConfig}
                let! handle = Async.StartChild(Worker.initWorker state resources maxTasks)
                logf "Worker loop started"
                
                logf "Service %s started" serviceId
                return! handle
            with ex ->
                logf "Service %s failed with %A" __.Id  ex
                return! Async.Raise ex
        }

    member __.Start() = Async.RunSynchronously(__.StartAsync())
