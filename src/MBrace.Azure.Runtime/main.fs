module internal Nessos.MBrace.Azure.Runtime.Main

    open Nessos.MBrace.Azure.Runtime.Config

    let maxConcurrentTasks = 10

    [<EntryPoint>]
    let main (args : string []) =
        try
            let conn = System.IO.File.ReadAllLines "/mbrace/conn.txt"
            let config = { StorageConnectionString = conn.[0]; ServiceBusConnectionString = conn.[1] }

            Nessos.MBrace.Azure.Runtime.Config.initialize(config)
            let runtime = Argument.toRuntime args
            Async.RunSynchronously (Worker.initWorker runtime maxConcurrentTasks)
        with e ->
            printfn "Unhandled exception : %O" e
            let _ = System.Console.ReadKey()
            1