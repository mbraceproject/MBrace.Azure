module internal Nessos.MBrace.Azure.Runtime.Main

    open System
    open Nessos.MBrace.Azure.Runtime.Config

    let maxConcurrentTasks = 10

    [<EntryPoint>]
    let main (args : string []) =
        try
            let selectEnv name =
                (Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.User),
                    Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.Machine))
                |> function | null, s | s, null | s, _ -> s

            let config = 
                { StorageConnectionString = selectEnv "AzureStorageConn";
                  ServiceBusConnectionString = selectEnv "AzureServiceBusConn" }

            Nessos.MBrace.Azure.Runtime.Config.initialize(config)
            let runtime = Argument.toRuntime args
            Async.RunSynchronously (Worker.initWorker runtime maxConcurrentTasks)
        with e ->
            printfn "Unhandled exception : %O" e
            let _ = System.Console.ReadKey()
            1