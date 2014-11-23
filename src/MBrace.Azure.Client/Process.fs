namespace Nessos.MBrace.Azure.Client

    open System
    open System.IO
    open System.Threading
    open Nessos.MBrace
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Azure.Runtime
    open Nessos.MBrace.Azure.Runtime.Common
    open Nessos.MBrace.Azure.Runtime.Resources
    open Nessos.MBrace.Runtime.Compiler

    [<AutoSerializable(false)>]
    type Process<'T> internal (pid : string, pmon : ProcessMonitor) =

        let proc = new Live<_>(
                    (fun () -> pmon.GetProcess(pid)), 
                    initial = Choice2Of2(exn("Process not initialized")), 
                    keepLast = true,
                    interval = 500,
                    stopf = function Choice1Of2 p when p.Completed -> true | _ -> false)

        member internal __.DistributedCancellationTokenSource = DistributedCancellationTokenSource.FromUri(new Uri(proc.Value.CancellationUri))

        member __.Id = pid
        member __.Name = proc.Value.Name
        
        member __.Created = proc.Value.TimeCreated
        
        member __.ExecutionTime =
            let s = if proc.Value.Completed 
                    then proc.Value.TimeCompleted 
                    else DateTime.UtcNow 
            s - proc.Value.TimeCreated
        member __.Completed = proc.Value.Completed
        
        member __.Kill () =__.DistributedCancellationTokenSource.Cancel()

        member __.AwaitResult () : 'T = __.AwaitResultAsync() |> Async.RunSynchronously
        member __.AwaitResultAsync () : Async<'T> = 
            async { 
                let rs : ResultCell<Result<'T>> = ResultCell.FromUri<_>(new Uri (proc.Value.ResultUri))
                let! r = rs.AwaitResult()
                return r.Value }

