namespace Nessos.MBrace.Azure.Runtime.Tests

open System.Threading

open NUnit.Framework
open FsUnit

open Nessos.MBrace
open Nessos.MBrace.Runtime

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
