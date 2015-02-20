namespace MBrace.Azure.Client

open MBrace.Azure.Runtime.Common
open MBrace.Runtime.Utils.PrettyPrinters
open System

type internal ProcessReporter() = 
    static let template : Field<ProcessRecord> list = 
        [ Field.create "Name" Left (fun p -> p.Name)
          Field.create "Process Id" Right (fun p -> p.Id)
          Field.create "State" Right (fun p -> p.State)
          Field.create "Completed" Left (fun p -> p.Completed)
          Field.create "Execution Time" Left (fun p -> if p.Completed then p.CompletionTime - p.InitializationTime  else DateTimeOffset.UtcNow - p.InitializationTime)
          Field.create "Jobs" Center (fun p -> sprintf "%3d / %3d / %3d / %3d"  p.ActiveJobs p.FaultedJobs p.CompletedJobs p.TotalJobs)
          Field.create "Result Type" Left (fun p -> p.TypeName) 
          Field.create "Start Time" Left (fun p -> p.InitializationTime)
          Field.create "Completion Time" Left (fun p -> if p.Completed then string p.CompletionTime else "N/A")
        ]
    
    static member Report(processes : ProcessRecord seq, title, borders) = 
        let ps = processes 
                 |> Seq.sortBy (fun p -> p.InitializationTime)
                 |> Seq.toList
        sprintf "%s\nJobs : Active / Faulted / Completed / Total\n" <| Record.PrettyPrint(template, ps, title, borders)

type internal WorkerReporter() = 
    static let template : Field<WorkerRecord> list = 
        let double_printer (value : Nullable<double>)   =
            if value.HasValue then sprintf "%.2f" value.Value else "N/A"
        [ Field.create "Id" Left (fun p -> p.Id)
          Field.create "Hostname" Left (fun p -> p.Hostname)
          Field.create "% CPU / Cores" Center (fun p -> sprintf "%s / %d" (double_printer p.CPU) p.ProcessorCount)
          Field.create "% Memory / Total(MB)" Center (fun p -> sprintf "%s / %s" <| double_printer p.Memory <| double_printer p.TotalMemory)
          Field.create "Network(ul/dl : kbps)" Center (fun n -> sprintf "%s / %s" <| double_printer n.NetworkUp <| double_printer n.NetworkDown)
          Field.create "Jobs" Center (fun p -> sprintf "%d / %d" p.ActiveJobs p.MaxJobs)
          Field.create "Process Id" Right (fun p -> p.ProcessId)
          Field.create "Initialization Time" Left (fun p -> p.InitializationTime) 
          Field.create "Heartbeat" Left (fun p -> p.Timestamp)
          Field.create "Is active" Left (fun p -> p.IsActive)
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
        [ Field.create "Source" Left (fun p -> p.PartitionKey)
          Field.create "Timestamp" Right (fun p -> let pt = p.Time in pt.ToString("ddMMyyyy HH:mm:ss.fff zzz"))
          Field.create "Message" Left (fun p -> p.Message) ]
    
    static member Report(logs : LogRecord seq, title, borders) = 
        let ls = logs 
                 |> Seq.sortBy (fun l -> l.Time, l.PartitionKey)
                 |> Seq.toList
        Record.PrettyPrint(template, ls, title, borders)
