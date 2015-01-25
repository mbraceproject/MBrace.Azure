namespace MBrace.Azure.Runtime.Common

open MBrace.Continuation

type ILogger =
    inherit ICloudLogger
    abstract Attach : ILogger -> unit
