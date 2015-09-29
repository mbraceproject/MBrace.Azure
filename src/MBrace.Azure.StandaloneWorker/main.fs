module internal MBrace.Azure.StandaloneWorker

open System
open System.Diagnostics

open MBrace.Runtime
open MBrace.Azure
open MBrace.Azure.Store
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Arguments

[<EntryPoint>]
let main (args : string []) =
    try
        ProcessConfiguration.InitAsWorker()
        let ps = Process.GetCurrentProcess()
        let cfg = ArgumentConfiguration.FromCommandLineArguments args
        let config = cfg.Configuration
        let workerId = 
            match cfg.WorkerName with
            | None -> sprintf "%s-%05d" <| System.Net.Dns.GetHostName() <| ps.Id
            | Some n -> n

        let svc = new Service(config, workerId)
        cfg.MaxWorkItems |> Option.iter (fun w -> svc.MaxConcurrentWorkItems <- w)
        Console.Title <- sprintf "%s(%d) : %s"  ps.ProcessName ps.Id svc.Id
        cfg.LogLevel |> Option.iter (fun l -> svc.LogLevel <- l)
        let _ = svc.AttachLogger(ConsoleLogger(showDate = true, useColors = true))
        svc.Run()
        0
    with e ->
        printfn "Unhandled exception : %O" e
        let _ = System.Console.ReadKey()
        1