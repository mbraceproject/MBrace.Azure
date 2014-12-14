module internal Nessos.MBrace.Azure.Runtime.Standalone.Program 
    open System
    open Nessos.MBrace.Azure.Runtime
    open Nessos.MBrace.Azure.Runtime.Common
    open System.Diagnostics
    open Nessos.MBrace.Azure.Store
    open Nessos.MBrace.Store

    [<EntryPoint>]
    let main (args : string []) =
        try
            let ps = Process.GetCurrentProcess()
            let cfg = Argument.toConfiguration args
            let config = cfg.Configuration

            let svc = new Service(config)
            svc.MaxConcurrentTasks <- cfg.MaxTasks
            Console.Title <- sprintf "%s(%d) : %s"  ps.ProcessName ps.Id svc.Id

            svc.AttachLogger(new ConsoleLogger())
            svc.Start() 
            0
        with e ->
            printfn "Unhandled exception : %O" e
            let _ = System.Console.ReadKey()
            1