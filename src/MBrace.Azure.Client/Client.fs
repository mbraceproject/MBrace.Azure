namespace Nessos.MBrace.Azure.Client

    open System.IO
    open System.Diagnostics
    open System.Threading

    open Nessos.MBrace
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Azure.Runtime
    open Nessos.MBrace.Azure.Runtime.Common
    open Nessos.MBrace.Runtime.Compiler
    open Nessos.MBrace.Azure.Runtime.Tasks

    #nowarn "40"

    /// MBrace Sample runtime client instance.
    type MBraceRuntime private () =
        static let mutable exe = None
        static let initWorkers (target : RuntimeState) (count : int) =
            if count < 1 then invalidArg "workerCount" "must be positive."
            let exe = MBraceRuntime.WorkerExecutable    
            let args = Argument.ofRuntime target
            let psi = new ProcessStartInfo(exe, args)
            psi.WorkingDirectory <- Path.GetDirectoryName exe
            psi.UseShellExecute <- true
            Array.init count (fun _ -> Process.Start psi)

        let mutable procs = [||]
        let getWorkerRefs () = procs |> Array.map (fun p -> new Worker(p) :> IWorkerRef)
        let state = RuntimeState.InitLocal ()
        
        /// Asynchronously execute a workflow on the distributed runtime.
        member __.RunAsync(workflow : Cloud<'T>, ?cancellationToken : CancellationToken, ?cleanup : bool) = async {
            let computation = CloudCompiler.Compile workflow
            let processId = System.Guid.NewGuid().ToString()
            let storageId = Storage.processIdToStorageId processId
            do! state.AssemblyExporter.UploadDependencies(computation.Dependencies)
            let! cts = state.ResourceFactory.RequestCancellationTokenSource(storageId)
            try
                cancellationToken |> Option.iter (fun ct -> ct.Register(fun () -> cts.Cancel()) |> ignore)
                let! resultCell = state.StartAsCell processId computation.Dependencies cts computation.Workflow
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

        /// Violently kills all worker nodes in the runtime
        member __.KillAllWorkers () = lock procs (fun () -> for p in procs do try p.Kill() with _ -> () ; procs <- [||])
        /// Gets all worker processes in the runtime
        member __.Workers = procs
        /// Appends count of new worker processes to the runtime.
        member __.AppendWorkers (count : int) =
            let newProcs = initWorkers state count
            lock procs (fun () -> procs <- Array.append procs newProcs)

        /// Initialize a new local runtime instance with supplied worker count.
        static member InitLocal(workerCount : int) =
            let client = new MBraceRuntime()
            client.AppendWorkers(workerCount)
            client

        static member GetHandle() = new MBraceRuntime()

        /// Gets or sets the worker executable location.
        static member WorkerExecutable
            with get () = match exe with None -> invalidOp "unset executable path." | Some e -> e
            and set path = 
                let path = Path.GetFullPath path
                if File.Exists path then exe <- Some path
                else raise <| FileNotFoundException(path)

        static member Configuration with set cfg = Configuration.Initialize(cfg)