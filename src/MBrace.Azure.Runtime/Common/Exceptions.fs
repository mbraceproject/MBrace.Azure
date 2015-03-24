namespace MBrace.Azure.Runtime

open System
open System.Runtime.Serialization

/// Base class for all MBrace.Azure exceptions.
[<Serializable>]
type RuntimeException =
    inherit Exception

    new () = { inherit Exception() }
    
    new (message : string, ?inner : exn) = 
        match inner with
        | None -> { inherit Exception(message) }
        | Some e -> { inherit Exception(message, e) }

    new (x : SerializationInfo, y : StreamingContext) = { inherit Exception(x, y) }

/// Exception indicating invalid Configuration.
[<Serializable>]
type InvalidConfigurationException =
    inherit RuntimeException

    new (message : string, inner) = { inherit RuntimeException(message, inner) }

    new (message) = { inherit RuntimeException(message) }

/// Exception indicating incompatible local and remote versions.
[<Serializable>]
type IncompatibleVersionException =
    inherit RuntimeException
    
    new (local, remote) = 
        { inherit RuntimeException(sprintf "Local metadata \n%s\nincompatible with remote metadata \n%s" local remote) }
    new (message) = 
        { inherit RuntimeException(message) }
    new (message : string, inner) = 
        { inherit RuntimeException(message, inner) }
