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

type LogEntity(pk : string, loggerType : string, message : string) =
    inherit TableEntity(pk, guid())
    member val Type = loggerType with get, set
    member val Message = message with get, set
    new () = new LogEntity(null, null, null)

type StorageLogger(table : string, loggerType : string, id : string) =
    let pk = "log"
    let maxMessageCount = 100

    let attached = new ConcurrentBag<ICloudLogger>()

    let log = 
        let logs = ConcurrentStack<string>()
        fun msg ->
            for l in attached do l.Log(msg)
            logs.Push(msg)
            let count = logs.Count
            if count >= maxMessageCount then
                let out = Array.zeroCreate count
                let count = logs.TryPopRange(out)
                let ys = Array.init count (fun i -> new LogEntity(pk, loggerType, out.[i]))
                Table.insertBatch<LogEntity> table ys
                |> Async.RunSynchronously

    //static let monitor : StorageLogger option ref = ref None 
    //static member Activated = monitor.Value.Value
    //static member Activate(table : string, loggerType : string, id : string) = 
        //if monitor.Value.IsSome then monitor.Value.Value
        //else
            //let m = new StorageLogger(table, loggerType, id)
            //monitor := Some m
            //m

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
        member x.Log(entry: string): unit = Console.WriteLine("{0} : {1}", DateTime.UtcNow, entry)

type CustomLogger (f : Action<string>) =
    interface ICloudLogger with
        member x.Log(entry: string): unit = f.Invoke(entry)
        