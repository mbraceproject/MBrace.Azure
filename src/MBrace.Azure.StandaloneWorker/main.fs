module internal MBrace.Azure.StandaloneWorker

open System
open MBrace.Azure
open MBrace.Azure.Runtime
open System.Diagnostics
open MBrace.Azure.Store
open MBrace.Runtime

[<EntryPoint>]
let main (args : string []) =
    try
        let ps = Process.GetCurrentProcess()
        let cfg = Arguments.Config.OfBase64Pickle(args, false)
        let config = cfg.Configuration
        let workerId = 
            match cfg.Name with
            | None -> sprintf "%s-%05d" <| System.Net.Dns.GetHostName() <| ps.Id
            | Some n -> n
        let svc = new Service(config, workerId)
        svc.MaxConcurrentJobs <- cfg.MaxJobs
        Console.Title <- sprintf "%s(%d) : %s"  ps.ProcessName ps.Id svc.Id
        cfg.LogLevel |> Option.iter (fun l -> svc.LogLevel <- l)
        let _ = svc.AttachLogger(ConsoleLogger(showDate = true, useColors = true))
        svc.Run()
        0
    with e ->
        printfn "Unhandled exception : %O" e
        let _ = System.Console.ReadKey()
        1