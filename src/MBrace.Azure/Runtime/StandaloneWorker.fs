namespace MBrace.Azure.Service

open System
open System.Diagnostics

open MBrace.Runtime
open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Arguments
open MBrace.Azure.Service

module StandaloneWorker =

    /// Recommended main method for running a standalone Azure worker 
    /// with provided command line arguments
    let main (args : string []) : int =
        try
            let cli = ArgumentConfiguration.FromCommandLineArguments args
            let config = Option.get cli.Configuration // successful parsing always yields 'Some' here
            let proc = Process.GetCurrentProcess()
            let workerId = 
                match cli.WorkerId with
                | None -> sprintf "%s-p%d" (System.Net.Dns.GetHostName()) proc.Id
                | Some n -> n

            let svc = new WorkerService(config, workerId)
            let _ = svc.AttachLogger(ConsoleLogger(showDate = true, useColors = true))
            cli.WorkingDirectory |> Option.iter (fun w -> svc.WorkingDirectory <- w)
            cli.MaxWorkItems |> Option.iter (fun w -> svc.MaxConcurrentWorkItems <- w)
            cli.HeartbeatInterval |> Option.iter (fun i -> svc.HeartbeatInterval <- i)
            cli.HeartbeatThreshold |> Option.iter (fun i -> svc.HeartbeatThreshold <- i)
            svc.LogFile <- defaultArg cli.LogFile "logs.txt"
            cli.LogLevel |> Option.iter (fun l -> svc.LogLevel <- l)

            Console.Title <- sprintf "%s(%d) : %s"  proc.ProcessName proc.Id svc.Id
            svc.Run()
            0

        with e ->
            printfn "Unhandled exception : %O" e
            let _ = System.Console.ReadKey()
            1