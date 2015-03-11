namespace MBrace.Azure.Runtime.Utilities

open MBrace.Runtime

[<AutoOpen>]
module LoggerExtensions =
    type ICloudLogger with
        member __.Logf fmt  = Printf.ksprintf __.Log fmt