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
open System.Collections.Concurrent

type LoggerType =
    | Worker of id : string
    | Client of id : string
    | Other of name : string * id : string with 
        override this.ToString() = 
            match this with
            | Worker id -> sprintf "worker:%s" id
            | Client id -> sprintf "client:%s" id
            | Other(name, id) -> sprintf "%s:%s" name id

type LogEntity(pk : string, loggerType : string, message : string) =
    inherit TableEntity(pk, guid())
    member val Type = loggerType with get, set
    member val Message = message with get, set
    new () = new LogEntity(null, null, null)

type StorageLogger(table : string, loggerType : LoggerType) =
    let pk = "log"

    let maxWaitTime = 5000
    let attached = new ConcurrentBag<ICloudLogger>()
    let logs = ConcurrentStack<string>()
    let flush () = async {
        let count = logs.Count
        if count > 0 then
            let out = Array.zeroCreate count
            let count = logs.TryPopRange(out)
            let ys = Array.init count (fun i -> new LogEntity(pk, string loggerType, out.[i]))
            return! Table.insertBatch<LogEntity> table ys
    }

    do  let rec loop _ = async {
            do! Async.Sleep maxWaitTime
            do! flush ()
            return! loop ()
        }
        Async.Start(loop ())

    let log msg = 
            for l in attached do l.Log(msg)
            logs.Push(msg)


    interface ICloudLogger with
        member x.Log(entry: string) : unit = log entry
        
    member __.Logf fmt = Printf.ksprintf log fmt

    member __.AsyncGetLogs () = Table.readBatch<LogEntity> table pk

    member __.Attach(logger : ICloudLogger) = attached.Add(logger)

type NullLogger () =
    interface ICloudLogger with
        member x.Log(entry: string): unit = ()

type ConsoleLogger () =
    interface ICloudLogger with
        member x.Log(entry: string): unit = Console.WriteLine("{0} : {1}", DateTime.UtcNow.ToString("ddMMyyyy HH:mm:ss"), entry)

type CustomLogger (f : Action<string>) =
    interface ICloudLogger with
        member x.Log(entry: string): unit = f.Invoke(entry)
        