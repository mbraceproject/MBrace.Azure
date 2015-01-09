namespace MBrace.Azure.Runtime.Standalone

open MBrace.Azure.Client
open System.Diagnostics
open System.IO
open MBrace.Continuation

/// BASE64 serialized argument parsing schema

module internal Argument =
    open MBrace.Azure.Runtime
    open MBrace.Runtime
    open MBrace.Runtime.Vagrant
    
    type Config = { Configuration : Configuration; MaxTasks : int}

    let ofConfiguration (config : Config) =
        Configuration.Initialize()
        let pickle = VagrantRegistry.Pickler.Pickle(config)
        System.Convert.ToBase64String pickle

    let toConfiguration (args : string []) =
        Configuration.Initialize()
        let bytes = System.Convert.FromBase64String(args.[0])
        VagrantRegistry.Pickler.UnPickle<Config> bytes

type private Helpers () =
    static member val exe = None with get, set

    static member InitWorkers (config : Argument.Config) (count : int) exe =
        MBrace.Azure.Runtime.Configuration.Activate(config.Configuration)
        if count < 1 then invalidArg "workerCount" "must be positive."  
        let args = Argument.ofConfiguration config 
        let psi = new ProcessStartInfo(exe, args)
        psi.WorkingDirectory <- Path.GetDirectoryName exe
        psi.UseShellExecute <- true
        Array.Parallel.init count (fun _ -> Process.Start psi)

[<AutoOpen>] 
module Extensions =
    open System
    open System.Runtime.CompilerServices

    [<Extension>]
    type Runtime with
        [<Extension>]
        /// Initialize a new local runtime instance with supplied worker count.
        static member Spawn(config, workerCount, ?maxTasks : int) =
            let cfg = { Argument.Configuration = config; Argument.MaxTasks = defaultArg maxTasks Environment.ProcessorCount}
            Helpers.InitWorkers cfg workerCount Runtime.WorkerExecutable
            |> ignore

        [<Extension>]
        /// Initialize a new local runtime instance with supplied worker count and return a handle.
        static member InitLocal(config, workerCount : int, ?maxTasks : int) =
            Runtime.Spawn(config, workerCount, ?maxTasks = maxTasks)
            Runtime.GetHandle(config)

        [<Extension>]
        /// Gets or sets the worker executable location.
        static member WorkerExecutable
            with get () = match Helpers.exe with None -> invalidOp "unset executable path." | Some e -> e
            and set path = 
                let path = Path.GetFullPath path
                if File.Exists path then Helpers.exe <- Some path
                else raise <| FileNotFoundException(path)