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
open System.Diagnostics

/// MBrace Runtime Service.
type Service (config : Configuration, maxTasks : int, storeConfig : CloudFileStoreConfiguration, serviceId : string) =

    let logger = new NullLogger() :> ILogger
    let logf fmt = Printf.ksprintf logger.Log fmt

    /// MBrace Runtime Service.
    new(config : Configuration, maxTasks : int, store : CloudFileStoreConfiguration) = new Service (config, maxTasks, store, guid())

    member __.Configuration = config
    member __.Id = serviceId
    member __.AttachLogger(l) = logger.Attach(l)
    member __.MaxConcurrentTasks = maxTasks
    member __.CloudFileStoreConfiguration = storeConfig
    member __.StartAsTask() : Tasks.Task = Async.StartAsTask(__.StartAsync()) :> _
    member __.StartAsTask(ct : CancellationToken) : Tasks.Task = Async.StartAsTask(__.StartAsync(), cancellationToken = ct) :> _
        
    member __.StartAsync() : Async<unit> =
        async {
            try
                logf "Starting Service %s" serviceId
                let sw = new Stopwatch() in sw.Start()

                logf "Activating Configuration"
                do! Configuration.Activate(config)

                logf "Creating storage logger"
                let storageLogger = new StorageLogger(config.ConfigurationId, config.DefaultLogTable, Worker(id = __.Id))
                logger.Attach(storageLogger)
                
                let serializer = Configuration.Serializer
                logf "Serializer : %s" serializer.Id

                logf "Initializing RuntimeState"
                let state = RuntimeState.FromConfiguration(config)

                state.TaskQueue.Affinity <- serviceId
                logf "Subscription for %s created" serviceId

                let atomConfig = { AtomProvider = AtomProvider.Create(config.ConfigurationId); DefaultContainer = config.DefaultTableOrContainer }
                logf "AtomProvider : %s" atomConfig.AtomProvider.Id

                let channelConfig = { ChannelProvider = ChannelProvider.Create(config.ConfigurationId); DefaultContainer = String.Empty }
                logf "ChannelProvider : %s" channelConfig.ChannelProvider.Id

                logf "CloudFileStore : %s" storeConfig.FileStore.Id

                let wmon = WorkerMonitor.Create(config)
                let! e = wmon.DeclareCurrent(serviceId)
                logf "Declared node %s : %d : %s" e.Hostname e.ProcessId (e :> IWorkerRef).Id
                
                Async.Start(wmon.HeartbeatLoop())
                logf "Started heartbeat loop" 

                let pmon = ProcessMonitor.Create(config)
                logf "ProcessMonitor created"

                let resources = 
                    resource { 
                        yield serializer
                        yield storeConfig
                        yield atomConfig
                        yield channelConfig
                        yield logger
                        yield wmon
                        yield pmon
                    }

                logf "MaxTasks : %d" maxTasks
                logf "Starting worker loop"
                let! handle = Async.StartChild(Worker.initWorker state resources maxTasks)
                logf "Worker loop started"
                
                sw.Stop()
                logf "Service %s started in %.3f seconds" serviceId sw.Elapsed.TotalSeconds
                return! handle
            with ex ->
                logf "Service %s failed with %A" __.Id  ex
                return! Async.Raise ex
        }

    member __.Start() = Async.RunSync(__.StartAsync())
