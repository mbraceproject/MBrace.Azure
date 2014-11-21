namespace Nessos.MBrace.Azure.Runtime.Standalone

open Nessos.MBrace.Azure.Client
open System.Diagnostics
open System.IO

/// BASE64 serialized argument parsing schema
module internal Argument =
    open Nessos.MBrace.Azure.Runtime
    open Nessos.MBrace.Runtime

    let ofConfiguration (runtime : Configuration) =
        Configuration.Initialize()
        let pickle = VagrantRegistry.Pickler.Pickle(runtime)
        System.Convert.ToBase64String pickle

    let toConfiguration (args : string []) =
        Configuration.Initialize()
        let bytes = System.Convert.FromBase64String(args.[0])
        VagrantRegistry.Pickler.UnPickle<Configuration> bytes

type private Helpers () =
    static member val exe = None with get, set

    static member InitWorkers config (count : int) exe =
        Nessos.MBrace.Azure.Runtime.Configuration.Activate(config)
        if count < 1 then invalidArg "workerCount" "must be positive."  
        let args = Argument.ofConfiguration config 
        let psi = new ProcessStartInfo(exe, args)
        psi.WorkingDirectory <- Path.GetDirectoryName exe
        psi.UseShellExecute <- true
        Array.init count (fun _ -> Process.Start psi)

[<AutoOpen>] 
module Extensions =
    [<System.Runtime.CompilerServices.Extension>]
    type Runtime with
        /// Initialize a new local runtime instance with supplied worker count.
        static member Spawn(config, workerCount) =
            Helpers.InitWorkers config workerCount Runtime.WorkerExecutable
            |> ignore
        
        /// Initialize a new local runtime instance with supplied worker count and return a handle.
        static member InitLocal(config, workerCount : int) =
            Runtime.Spawn(config, workerCount)
            Runtime.GetHandle(config)

        /// Gets or sets the worker executable location.
        static member WorkerExecutable
            with get () = match Helpers.exe with None -> invalidOp "unset executable path." | Some e -> e
            and set path = 
                let path = Path.GetFullPath path
                if File.Exists path then Helpers.exe <- Some path
                else raise <| FileNotFoundException(path)