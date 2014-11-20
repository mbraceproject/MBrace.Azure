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
    type Runtime private (state) =
        static let mutable exe = None
        static let initWorkers (target : RuntimeState) (count : int) =
            if count < 1 then invalidArg "workerCount" "must be positive."
            let exe = Runtime.WorkerExecutable    
            let args = Argument.ofRuntime target
            let psi = new ProcessStartInfo(exe, args)
            psi.WorkingDirectory <- Path.GetDirectoryName exe
            psi.UseShellExecute <- true
            Array.init count (fun _ -> Process.Start psi)

        let id = guid()
        let wmon = WorkerMonitor.Activate(Storage.defaultStorageId)
        let logger = new StorageLogger(Storage.defaultLogId, "client", id)
        
        /// Asynchronously execute a workflow on the distributed runtime.
        member __.RunAsync(workflow : Cloud<'T>, ?cancellationToken : CancellationToken, ?cleanup : bool) = async {
            let computation = CloudCompiler.Compile workflow
            let processId = System.Guid.NewGuid().ToString()
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
        static member InitLocal(workerCount : int) =
            let state = RuntimeState.InitLocal ()
            let client = new Runtime(state)
            let newProcs = initWorkers state workerCount
            client

        static member GetHandle() = 
            let state = RuntimeState.InitLocal ()
            new Runtime(state)

        /// Gets or sets the worker executable location.
        static member WorkerExecutable
            with get () = match exe with None -> invalidOp "unset executable path." | Some e -> e
            and set path = 
                let path = Path.GetFullPath path
                if File.Exists path then exe <- Some path
                else raise <| FileNotFoundException(path)

        static member Configuration with set cfg = Configuration.Initialize(cfg)