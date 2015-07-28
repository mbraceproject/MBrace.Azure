module internal MBrace.Azure.Worker.Standalone

    open System
    open MBrace.Azure
    open MBrace.Azure.Runtime
    open System.Diagnostics
    open MBrace.Azure.Store
    open MBrace.Store
    open MBrace.Runtime

    [<EntryPoint>]
    let main (args : string []) =
        try
            let ps = Process.GetCurrentProcess()
            let cfg = Arguments.Config.OfBase64Pickle args
            let config = cfg.Configuration
            let workerId = sprintf "%s-%05d" <| System.Net.Dns.GetHostName() <| Diagnostics.Process.GetCurrentProcess().Id
            let svc = new Service(config, workerId)
            svc.MaxConcurrentJobs <- cfg.MaxTasks
            Console.Title <- sprintf "%s(%d) : %s"  ps.ProcessName ps.Id svc.Id

            svc.AttachLogger(new ConsoleLogger(true))
            svc.Run()
            0
        with e ->
            printfn "Unhandled exception : %O" e
            let _ = System.Console.ReadKey()
            1