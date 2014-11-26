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

type ILogger =
    inherit ICloudLogger
    abstract Attach : ILogger -> unit

type LoggerType =
    | Worker of id : string
    | Client of id : string
    | Other of name : string * id : string with 
        override this.ToString() = 
            match this with
            | Worker id -> sprintf "worker:%s" id
            | Client id -> sprintf "client:%s" id
            | Other(name, id) -> sprintf "%s:%s" name id

type LogEntity(pk : string, loggerType : string, message : string, time : DateTime) =
    inherit TableEntity(pk, guid())
    member val Type = loggerType with get, set
    member val Message = message with get, set
    member val Time = time with get, set
    new () = new LogEntity(null, null, null, Unchecked.defaultof<_>)

type LoggerBase () =
    let attached = new ConcurrentBag<ILogger>()

    abstract member Log : string -> unit
    override __.Log(msg) = 
        for l in attached do
            l.Log(msg)

    abstract member Attach : ILogger -> unit
    override __.Attach(logger) = attached.Add(logger)

    interface ILogger with
        member __.Attach(entry : ILogger) = __.Attach(entry)
        member __.Log entry = __.Log entry
  

type StorageLogger(config, table : string, loggerType : LoggerType) =
    inherit LoggerBase () with
    
        let pk = "log"

        let maxWaitTime = 5000
        let logs = ConcurrentStack<LogEntity>()
        let flush () = async {
            let count = logs.Count
            if count > 0 then
                let out = Array.zeroCreate count
                let count = logs.TryPopRange(out)
                return! Table.insertBatch<LogEntity> config table out
        }

        do  let rec loop _ = async {
                do! Async.Sleep maxWaitTime
                do! flush ()
                return! loop ()
            }
            Async.Start(loop ())

        let log msg = 
            let e = new LogEntity(pk, string loggerType, msg, DateTime.UtcNow)
            logs.Push(e)

        override __.Log(entry: string) : unit = log entry; base.Log(entry)

        member __.Logf fmt = Printf.ksprintf __.Log fmt

        member __.AsyncGetLogs () = Table.readBatch<LogEntity> config table pk


type NullLogger () =
    inherit LoggerBase () 

type ConsoleLogger () =
    inherit LoggerBase () with
        override x.Log(entry : string): unit = 
            Console.WriteLine("{0} : {1}", DateTime.UtcNow.ToString("ddMMyyyy HH:mm:ss.fff zzz"), entry)
            base.Log(entry)

type CustomLogger (f : Action<string>) =
    inherit LoggerBase () with
        override x.Log(entry : string): unit = 
            f.Invoke(entry)
            base.Log(entry)
        