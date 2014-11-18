module internal Nessos.MBrace.Azure.Runtime.Main

    open System
    open Nessos.MBrace.Azure.Runtime

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

            Service.Configuration <- config
            printfn "Configuration initialized..."
            let state = Argument.toRuntime args.[0]
            printfn "State initialized...\nStarting Runtime service..."
            Service.AsyncStart(state, (fun s -> Console.WriteLine s), 10) |> Async.RunSynchronously
            0
        with e ->
            printfn "Unhandled exception : %O" e
            let _ = System.Console.ReadKey()
            1