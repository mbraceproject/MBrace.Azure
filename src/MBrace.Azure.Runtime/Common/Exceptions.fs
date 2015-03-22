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
type InvalidConfigurationException(message : string, inner) =
    inherit RuntimeException(message, inner)

/// Exception indicating incompatible local and remote versions.
type IncompatibleVersionException(localVersion, remoteVersion) =
    inherit RuntimeException(sprintf "Local version '%s', incompatible with remote version '%s'" localVersion remoteVersion)
    
    member val LocalVersion = localVersion
    member val RemoteVersion = remoteVersion
