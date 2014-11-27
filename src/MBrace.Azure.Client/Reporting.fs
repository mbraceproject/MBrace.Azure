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

type internal ProcessReporter() = 
    
    static let template : Field<Process> list = 
        [ Field.create "Name" Left (fun p -> p.Name)
          Field.create "Process Id" Right (fun p -> p.Id)
          Field.create "State" Right (fun p -> p.ProcessEntity.Value.State)
          Field.create "Completed" Left (fun p -> p.Completed)
          Field.create "Start Time" Left (fun p -> p.InitializationTime)
          Field.create "Execution Time" Left (fun p -> p.ExecutionTime)
          Field.create "Result Type" Left (fun p -> p.Type) ]
    
    static member Report(processes : Process list, title, borders) = Record.PrettyPrint(template, processes, title, borders)

type internal WorkerReporter() = 
    
    static let template : Field<WorkerRef> list = 
        [ Field.create "Id" Left (fun p -> (p :> IWorkerRef).Id)
          Field.create "Initialization Time" Left (fun p -> p.InitializationTime) 
          Field.create "Hostname" Left (fun p -> p.Hostname)
          Field.create "Process Id" Right (fun p -> p.ProcessId)
          Field.create "Process Name" Right (fun p -> p.ProcessName) ]
    
    static member Report(processes : WorkerRef list, title, borders) = Record.PrettyPrint(template, processes, title, borders)

type internal LogReporter() = 
    
    static let template : Field<LogEntity> list = 
        [ Field.create "Source" Left (fun p -> p.Type)
          Field.create "Timestamp" Right (fun p -> p.Time)
          Field.create "Message" Left (fun p -> p.Message) ]
    
    static member Report(processes : LogEntity list, title, borders) = Record.PrettyPrint(template, processes, title, borders)
