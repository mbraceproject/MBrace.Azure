namespace Nessos.MBrace.Azure.Runtime.Common

open System
open System.Runtime.Serialization
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging
open Nessos.MBrace.Azure.Runtime

open System.Net
open System.Diagnostics
open Nessos.MBrace
open Nessos.MBrace.Runtime

type LogEntity(pk : string, loggerType : string, message : string) =
    inherit TableEntity(pk, guid())
    member val Type = loggerType with get, set
    member val Message = message with get, set
    new () = new LogEntity(null, null, null)

type StorageLogger private (table : string, loggerType : string, id : string) =
    let pk = "log"

    // TODO :
    static let monitor : StorageLogger option ref = ref None 
    let func : (string -> unit) ref = ref ignore

    static member Activated = monitor.Value.Value

    static member Activate(table : string, loggerType : string, id : string) = 
        if monitor.Value.IsSome then monitor.Value.Value
        else
            let m = new StorageLogger(table, loggerType, id)
            monitor := Some m
            m

    interface ICloudLogger with
        member x.Log(entry: string) : unit = Async.RunSynchronously <| x.AsyncLog(entry)
        

    member __.AsyncLog(message : string) = 
        func.Value message
        Table.insert<LogEntity> table <| new LogEntity(pk, loggerType, message)

    member __.AsyncLogf fmt = Printf.ksprintf (__.AsyncLog) fmt

    member __.AsyncGetLogs () = Table.readBatch<LogEntity> table pk

    member __.Attach(f) = func := fun s -> f(s)