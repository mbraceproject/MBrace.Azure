namespace MBrace.Azure.Runtime.Utilities

open MBrace.Core.Internals
open MBrace.Runtime.Store
open MBrace.Store.Internals

[<AutoOpen>]
module LoggerExtensions =
    type ICloudLogger with
        member __.Logf fmt  = Printf.ksprintf __.Log fmt