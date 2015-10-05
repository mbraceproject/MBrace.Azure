namespace MBrace.Azure.Service

open System
open System.Diagnostics

open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Arguments
open MBrace.Azure.Service

module StandaloneWorker =

    /// Recommended main method for running a standalone Azure worker 
    /// with provided command line arguments
    let main (args : string []) : int =
        try
            let cfg = ArgumentConfiguration.FromCommandLineArguments args
            let proc = Process.GetCurrentProcess()
            let workerId = 
                match cfg.WorkerName with
                | None -> sprintf "%s-pid%d" (System.Net.Dns.GetHostName()) proc.Id
                | Some n -> n

            let svc = new WorkerService(cfg.Configuration, workerId)
            let _ = svc.AttachLogger(ConsoleLogger(showDate = true, useColors = true))
            cfg.WorkingDirectory |> Option.iter (fun w -> svc.WorkingDirectory <- w)
            cfg.MaxWorkItems |> Option.iter (fun w -> svc.MaxConcurrentWorkItems <- w)
            cfg.HeartbeatInterval |> Option.iter (fun i -> svc.HeartbeatInterval <- i)
            cfg.HeartbeatThreshold |> Option.iter (fun i -> svc.HeartbeatThreshold <- i)
            svc.LogFile <- defaultArg cfg.LogFile "logs.txt"
            cfg.LogLevel |> Option.iter (fun l -> svc.LogLevel <- l)

            Console.Title <- sprintf "%s(%d) : %s"  proc.ProcessName proc.Id svc.Id
            svc.Run()
            0

        with e ->
            printfn "Unhandled exception : %O" e
            let _ = System.Console.ReadKey()
            1