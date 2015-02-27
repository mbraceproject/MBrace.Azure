namespace MBrace.Azure.Runtime.Tests

open System.Threading

open NUnit.Framework
open FsUnit

open MBrace
open MBrace.Runtime
open MBrace.Azure.Runtime
open MBrace.Azure.Client

#nowarn "445"

module Utils =
    open System

    let selectEnv name =
        (Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.User),
          Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.Machine),
            Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.Process))
        |> function 
           | s, _, _ when not <| String.IsNullOrEmpty(s) -> s
           | _, s, _ when not <| String.IsNullOrEmpty(s) -> s
           | _, _, s when not <| String.IsNullOrEmpty(s) -> s
           | _ -> failwith "Variable not found"

module Choice =

    let shouldEqual (value : 'T) (input : Choice<'T, exn>) = 
        match input with
        | Choice1Of2 v' -> should equal value v'
        | Choice2Of2 e -> should equal value e

    let shouldMatch (pred : 'T -> bool) (input : Choice<'T, exn>) =
        match input with
        | Choice1Of2 t when pred t -> ()
        | Choice1Of2 t -> raise <| new AssertionException(sprintf "value '%A' does not match predicate." t)
        | Choice2Of2 e -> should be instanceOfType<'T> e

    let shouldFailwith<'T, 'Exn when 'Exn :> exn> (input : Choice<'T, exn>) = 
        match input with
        | Choice1Of2 t -> should be instanceOfType<'Exn> t
        | Choice2Of2 e -> should be instanceOfType<'Exn> e


type RuntimeSession(config : MBrace.Azure.Configuration) =

    let mutable state = None

    member __.Start () = 
        let rt = Runtime.GetHandle(config)
        let logger = Common.ConsoleLogger() :> ICloudLogger
        rt.AttachClientLogger logger
        state <- Some (rt, logger)
        do System.Threading.Thread.Sleep 2000

    member __.Stop () =
        state |> Option.iter (fun (r,l) -> r.Reset(false, true))
        state <- None

    member __.Runtime =
        match state with
        | None -> invalidOp "MBrace runtime not initialized."
        | Some (r,_) -> r

    member __.Logger =
        match state with
        | None -> invalidOp "MBrace runtime not initialized."
        | Some (_,l) -> l