namespace MBrace.Azure.Runtime.Utilities

open MBrace.Core.Internals

[<AutoOpen>]
module LoggerExtensions =
    type ICloudLogger with
        member __.Logf fmt  = Printf.ksprintf __.Log fmt