module internal Nessos.MBrace.Azure.Runtime.Main

    open System
    open Nessos.MBrace.Azure.Runtime
    open Nessos.MBrace.Azure.Runtime.Common

    [<EntryPoint>]
    let main (args : string []) =
        try
            let selectEnv name =
                (Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.User),
                    Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.Machine))
                |> function | null, s | s, null | s, _ -> s

            let config = 
                { Configuration.Default with
                    StorageConnectionString = selectEnv "AzureStorageConn";
                    ServiceBusConnectionString = selectEnv "AzureServiceBusConn" }

            let svc = new Service(config, 10)

            let slogger = new StorageLogger(config.DefaultLogTable, "worker", svc.Id)
            let clogger = new ConsoleLogger() in slogger.Attach(clogger)
            svc.Logger <- slogger
            svc.Start() 
            0
        with e ->
            printfn "Unhandled exception : %O" e
            let _ = System.Console.ReadKey()
            1