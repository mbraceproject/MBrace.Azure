namespace Nessos.MBrace.Azure.Client

    open System.IO
    open System.Diagnostics
    open System.Threading

    open Nessos.MBrace
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Azure.Runtime
    open Nessos.MBrace.Azure.Runtime.Common
    open Nessos.MBrace.Runtime.Compiler

    #nowarn "40"

    /// MBrace Sample runtime client instance.
    type Runtime private (config : Configuration) =
        let id = guid()
        do Configuration.Activate(config)
        let wmon = WorkerMonitor.Activate(config.DefaultTableOrContainer)
        let logger = new StorageLogger(config.DefaultLogTable, "client", id)
        do logger.Attach(new ConsoleLogger()) // remove
        let state = RuntimeState.FromConfiguration(config)
        
        member private __.RuntimeState = state

        /// Asynchronously execute a workflow on the distributed runtime.
        member __.RunAsync(workflow : Cloud<'T>, ?cancellationToken : CancellationToken, ?cleanup : bool) = async {
            let computation = CloudCompiler.Compile workflow
            let processId = guid()
            logger.Logf "Creating process %s" processId
            let storageId = Storage.processIdToStorageId processId
            logger.Logf "Uploading dependencies %O" computation.Dependencies
            do! state.AssemblyExporter.UploadDependencies(computation.Dependencies)
            logger.Logf "Creating DistributedCancellationToken"
            let! cts = state.ResourceFactory.RequestCancellationTokenSource(storageId)
            try
                cancellationToken |> Option.iter (fun ct -> ct.Register(fun () -> cts.Cancel()) |> ignore)
                logger.Logf "Starting computation"
                let! resultCell = state.StartAsCell processId computation.Dependencies cts computation.Workflow
                logger.Logf "Waiting for result"
                let! result = resultCell.AwaitResult()
                return result.Value
            finally
                cts.Cancel ()
                let cleanup = defaultArg cleanup false
                if cleanup then Async.RunSynchronously <| Storage.clearProcessFolder storageId
        }

        /// Execute a workflow on the distributed runtime as task.
        member __.RunAsTask(workflow : Cloud<'T>, ?cancellationToken : CancellationToken, ?cleanup : bool) =
            let asyncwf = __.RunAsync(workflow, ?cancellationToken = cancellationToken, ?cleanup = cleanup)
            Async.StartAsTask(asyncwf)

        /// Execute a workflow on the distributed runtime synchronously
        member __.Run(workflow : Cloud<'T>, ?cancellationToken : CancellationToken, ?cleanup : bool) =
            __.RunAsync(workflow, ?cancellationToken = cancellationToken, ?cleanup = cleanup) |> Async.RunSynchronously

        member __.GetWorkers () = Async.RunSynchronously <| wmon.GetWorkers()

        member __.GetLogs () = Async.RunSynchronously <| logger.AsyncGetLogs()

        static member GetHandle(config : Configuration) = 
            new Runtime(config)

