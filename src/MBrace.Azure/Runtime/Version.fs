namespace MBrace.Azure.Runtime

open System
open System.IO
open System.Reflection
open MBrace.Azure

module internal ReleaseInfo =

    /// Get Release Signature string from assembly metadata.
    let signatureString () =
        let asm = Assembly.GetExecutingAssembly()
        let attributes = 
            asm.GetCustomAttributes<AssemblyMetadataAttribute>()
            |> Seq.map (fun ma -> ma.Key, ma.Value)
            |> Map.ofSeq
        attributes.["Release Signature"]

    /// Get version.
    let localVersion = typeof<Configuration>.Assembly.GetName().Version

type Metadata =
    { 
        Version : Version
        ConfigurationId : ClusterState 
    }
    
[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module internal Metadata =

    let private jsonSerializer = lazy(new Nessos.FsPickler.Json.JsonSerializer(omitHeader = true, indent = false))

    let compareConfigurations (local : Metadata) (remote : Metadata) =
        if local.ConfigurationId <> remote.ConfigurationId then
            let msg = sprintf "Configuration mismatch. Given configuration %+A does not match remote configuration %+A" local.ConfigurationId remote.ConfigurationId
            raise <| FormatException(msg)

    let compare (local : Metadata) (remote : Metadata) =
        if local <> remote then
            raise <| FormatException(sprintf "Incompatible metadata, received %A, expected %A" remote local)

    let toString (version : Version) (config : ClusterState) =
        let metadata = { Version = version; ConfigurationId = config }
        jsonSerializer.Value.PickleToString metadata

    let fromString (metadata : string) =
        try jsonSerializer.Value.UnPickleOfString<Metadata> metadata
        with e -> raise <| FormatException(sprintf "Failed to deserialize metadata %s" metadata, e)