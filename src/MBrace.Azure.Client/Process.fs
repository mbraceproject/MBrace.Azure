namespace Nessos.MBrace.Azure.Client

    open System.IO
    open System.Threading

    open Nessos.MBrace
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Azure.Runtime
    open Nessos.MBrace.Azure.Runtime.Common
    open Nessos.MBrace.Runtime.Compiler

    #nowarn "40"

    [<AutoSerializable(false)>]
    type Process<'T> internal (procId : string, cts : CancellationTokenSource, task : Tasks.Task<'T>, pmon : ProcessMonitor) =
        member __.Id = procId
        member __.Kill () = cts.Cancel()
        member __.AsTask() = task
        member __.AwaitResult () = task.Wait() 
        