module internal Nessos.MBrace.Azure.Runtime.Standalone.Program 
    open System
    open Nessos.MBrace.Azure.Runtime
    open Nessos.MBrace.Azure.Runtime.Common

    [<EntryPoint>]
    let main (args : string []) =
        try
            let config = Argument.toConfiguration args

            let svc = new Service(config, 10)
            let slogger = new StorageLogger(config.DefaultLogTable, Worker(id = svc.Id))
            let clogger = new ConsoleLogger() in slogger.Attach(clogger)
            svc.Logger <- slogger
            svc.Start() 
            0
        with e ->
            printfn "Unhandled exception : %O" e
            let _ = System.Console.ReadKey()
            1