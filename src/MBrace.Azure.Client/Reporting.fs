namespace Nessos.MBrace.Azure.Client

open Nessos.MBrace
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Common
open Nessos.MBrace.Azure.Runtime.Resources
open Nessos.MBrace.Runtime
open Nessos.MBrace.Runtime.Compiler
open Nessos.MBrace.Runtime.Utils.PrettyPrinters
open System
open System.IO
open System.Threading
open Microsoft.FSharp.Linq.NullableOperators

type internal ProcessReporter() = 
    
    static let template : Field<ProcessRecord> list = 
        [ Field.create "Name" Left (fun p -> p.Name)
          Field.create "Process Id" Right (fun p -> p.Id)
          Field.create "State" Right (fun p -> p.State)
          Field.create "Completed" Left (fun p -> p.Completed)
          Field.create "Start Time" Left (fun p -> p.InitializationTime)
          Field.create "Execution Time" Left (fun p -> if p.Completed then p.CompletionTime - p.InitializationTime  else DateTimeOffset.UtcNow - p.InitializationTime)
          Field.create "Completion Time" Left (fun p -> if p.Completed then string p.CompletionTime else "N/A")
          Field.create "Result Type" Left (fun p -> p.TypeName) 
        ]
    
    static member Report(processes : ProcessRecord seq, title, borders) = 
        let ps = processes 
                 |> Seq.sortBy (fun p -> p.InitializationTime)
                 |> Seq.toList
        Record.PrettyPrint(template, ps, title, borders)

type internal WorkerReporter() = 
    static let template : Field<WorkerRecord> list = 
        let double_printer (value : Nullable<double>)   =
            if value.HasValue then sprintf "%.2f" value.Value else "N/A"
        let int_printer (value : Nullable<_>)   =
            if value.HasValue then sprintf "%d" value.Value else "0"
        [ Field.create "Id" Left (fun p -> p.Id)
          Field.create "Hostname" Left (fun p -> p.Hostname)
          Field.create "% CPU" Right (fun p -> double_printer p.CPU)
          Field.create "% Memory" Right (fun p -> double_printer p.Memory)
          Field.create "Total Memory(MB)" Right (fun p -> double_printer p.TotalMemory)
          Field.create "Network(ul/dl : kbps)"  Right (fun n -> sprintf "%s / %s" <| double_printer n.NetworkUp <| double_printer n.NetworkDown)
          Field.create "Active/Completed Tasks" Center (fun p -> sprintf "%s / %s" <| int_printer p.ActiveTasks <| int_printer p.CompletedTasks)
          Field.create "Cores" Right (fun p -> p.ProcessorCount)
          Field.create "Process Id" Right (fun p -> p.ProcessId)
          Field.create "Initialization Time" Left (fun p -> p.InitializationTime) 
          Field.create "Heartbeat" Left (fun p -> p.Timestamp)
        ]
    
    static member Report(workers : WorkerRecord seq, title, borders) = 
            // TODO : print summary
//        let (cpu, memory, totalMemory, ul, dl, n) = 
//            workers
//            |> Seq.fold(fun (cpu, memory, totalMemory, ul, dl, n) w ->
//                w.CPU ?+? cpu, 
//                memory ?+? w.Memory ?*? w.TotalMemory, 
//                totalMemory ?+? w.TotalMemory, 
//                ul ?+? w.NetworkUp, 
//                dl ?+? w.NetworkDown,
//                n + 1) 
//                (Nullable<_>(0.0), Nullable<_>(0.0), Nullable<_>(0.0), Nullable<_>(0.0), Nullable<_>(0.0), 0)
        let ws = workers
                 |> Seq.sortBy (fun w -> w.InitializationTime)
                 |> Seq.toList
        Record.PrettyPrint(template, ws, title, borders)

type internal LogReporter() = 
    
    static let template : Field<LogRecord> list = 
        [ Field.create "Source" Left (fun p -> p.Type)
          Field.create "Timestamp" Right (fun p -> p.Time)
          Field.create "Message" Left (fun p -> p.Message) ]
    
    static member Report(logs : LogRecord seq, title, borders) = 
        let ls = logs 
                 |> Seq.sortBy (fun l -> l.Time, l.Type)
                 |> Seq.toList
        Record.PrettyPrint(template, ls, title, borders)
