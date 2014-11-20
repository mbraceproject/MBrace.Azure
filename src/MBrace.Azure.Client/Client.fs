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
        static let mutable exe = None
        static let initWorkers (target : RuntimeState) (count : int) =
            if count < 1 then invalidArg "workerCount" "must be positive."
            let exe = Runtime.WorkerExecutable    
            let psi = new ProcessStartInfo(exe)
            psi.WorkingDirectory <- Path.GetDirectoryName exe
            psi.UseShellExecute <- true
            Array.init count (fun _ -> Process.Start psi)

        let id = guid()
        do Configuration.Activate(config)
        let wmon = WorkerMonitor.Activate(config.DefaultTable)
        let logger = new StorageLogger(config.DefaultLogTable, "client", id)
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
            let! cts = state.ResourceFactory.RequestCancellationTokenSource(storageId)
            try
                cancellationToken |> Option.iter (fun ct -> ct.Register(fun () -> cts.Cancel()) |> ignore)
                let! resultCell = state.StartAsCell processId computation.Dependencies cts computation.Workflow
                logger.Logf "Computation started"
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

        /// Initialize a new local runtime instance with supplied worker count.
        static member InitLocal(config, workerCount : int) =
            let runtime = new Runtime(config)
            let newProcs = initWorkers runtime.RuntimeState workerCount
            runtime

        static member GetHandle(config : Configuration) = 
            new Runtime(config)

        /// Gets or sets the worker executable location.
        static member WorkerExecutable
            with get () = match exe with None -> invalidOp "unset executable path." | Some e -> e
            and set path = 
                let path = Path.GetFullPath path
                if File.Exists path then exe <- Some path
                else raise <| FileNotFoundException(path)
