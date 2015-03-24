namespace MBrace.Azure.Runtime

open System.Reflection
open System
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

    /// Get version string
    let localVersion = Assembly.GetExecutingAssembly().GetName().Version
  
type Metadata =
    { Version : Version
      ConfigurationId : ConfigurationId }
    
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module internal Metadata =
    open Nessos.FsPickler
    open System.IO

    let compare (local : Metadata) (remote : Metadata) =
        if local <> remote then
            raise <| IncompatibleVersionException(string local, string remote)

    let toString (version : Version) (configurationId : ConfigurationId) =
        let metadata = { Version = version; ConfigurationId = configurationId }
        use sw = new StringWriter()
        let xml = FsPickler.CreateXml()
        xml.Serialize(sw, metadata)
        sw.ToString()

    let fromString (metadata : string) =
        let xml = FsPickler.CreateXml()
        use sr = new StringReader(metadata)
        try
           xml.Deserialize<Metadata>(sr)
        with ex ->
            raise <| IncompatibleVersionException(sprintf "Failed to deserialize metadata %s" metadata, ex)