module internal Nessos.MBrace.Azure.Runtime.Standalone.Program 
    open System
    open Nessos.MBrace.Azure.Runtime
    open Nessos.MBrace.Azure.Runtime.Common
    open System.Diagnostics

    [<EntryPoint>]
    let main (args : string []) =
        try
            let ps = Process.GetCurrentProcess()
            let config = Argument.toConfiguration args

            let svc = new Service(config, 10)
            
            Console.Title <- sprintf "%s(%d) : %s"  ps.ProcessName ps.Id svc.Id

            let slogger = new StorageLogger(config.DefaultLogTable, Worker(id = svc.Id))
            slogger.Attach(new ConsoleLogger())
            svc.AttachLogger(slogger)
            svc.Start() 
            0
        with e ->
            printfn "Unhandled exception : %O" e
            let _ = System.Console.ReadKey()
            1