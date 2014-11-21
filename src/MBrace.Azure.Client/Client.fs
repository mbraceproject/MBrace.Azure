namespace Nessos.MBrace.Azure.Client

    open System.IO
    open System.Threading

    open Nessos.MBrace
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Azure.Runtime
    open Nessos.MBrace.Azure.Runtime.Common
    open Nessos.MBrace.Runtime.Compiler

    #nowarn "40"

    /// MBrace Sample runtime client instance.
    [<AutoSerializable(false)>]
    type Runtime private (config : Configuration) =
        let id = guid()
        do Configuration.Activate(config)
        let wmon = WorkerMonitor.Activate(config.DefaultTableOrContainer)
        let logger = new StorageLogger(config.DefaultLogTable, "client", id)
        do logger.Attach(new ConsoleLogger()) // remove
        let state = RuntimeState.FromConfiguration(config)
        
        member private __.RuntimeState = state

//        member __.CreateProcess(workflow : Cloud<'T>, ?cancellationToken : CancellationToken) : Async<Process<'T>> =
//            async {
//                let computation = CloudCompiler.Compile workflow
//                let processId = guid()
//                logger.Logf "Creating process %s" processId
//                let storageId = Storage.processIdToStorageId processId
//                logger.Logf "Uploading dependencies %O" computation.Dependencies
//                do! state.AssemblyExporter.UploadDependencies(computation.Dependencies)
//                logger.Logf "Creating DistributedCancellationToken"
//                let! cts = state.ResourceFactory.RequestCancellationTokenSource(storageId)
//                try
//                    cancellationToken |> Option.iter (fun ct -> ct.Register(fun () -> cts.Cancel()) |> ignore)
//                    logger.Logf "Starting computation"
//                    let! resultCell = state.StartAsCellRoot processId computation.Dependencies cts computation.Workflow
//                    let task = Async.StartAsTask(resultCell.AwaitResult())
//                    return Process(processId, cts.GetLocalCancellationToken()., task, state.ResourceFactory.RequestProcessMonitor())
//                finally
//                    cts.Cancel ()
//            }
            
        /// Asynchronously execute a workflow on the distributed runtime.
        member __.RunAsync(workflow : Cloud<'T>, ?cancellationToken : CancellationToken) = async {
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
                let! resultCell = state.StartAsCellRoot processId computation.Dependencies cts computation.Workflow
                logger.Logf "Waiting for result"
                let! result = resultCell.AwaitResult()
                return result.Value
            finally
                cts.Cancel ()
        }

        /// Execute a workflow on the distributed runtime as task.
        member __.RunAsTask(workflow : Cloud<'T>, ?cancellationToken : CancellationToken) =
            let asyncwf = __.RunAsync(workflow, ?cancellationToken = cancellationToken)
            Async.StartAsTask(asyncwf)

        /// Execute a workflow on the distributed runtime synchronously
        member __.Run(workflow : Cloud<'T>, ?cancellationToken : CancellationToken, ?cleanup : bool) =
            __.RunAsync(workflow, ?cancellationToken = cancellationToken) |> Async.RunSynchronously

        member __.GetWorkers () = Async.RunSynchronously <| wmon.GetWorkers()

        member __.GetLogs () = Async.RunSynchronously <| logger.AsyncGetLogs()

        static member GetHandle(config : Configuration) = new Runtime(config)

