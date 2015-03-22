namespace MBrace.Azure.Runtime

module internal ReleaseInfo =
    open System.Reflection
    open System

    /// Get Release Signature string from assembly metadata.
    let signatureString () =
        let asm = Assembly.GetExecutingAssembly()
        let attributes = 
            asm.GetCustomAttributes<AssemblyMetadataAttribute>()
            |> Seq.map (fun ma -> ma.Key, ma.Value)
            |> Map.ofSeq
        attributes.["Release Signature"]

    /// Get version string
    let localVersion = Assembly.GetExecutingAssembly().GetName().Version.ToString()
    
    /// Compare version strings and throw if not compatible.
    let compareWithVersion (remoteVersion : string) =
        let local = Version.Parse(localVersion)
        match Version.TryParse(remoteVersion) with
        | true, remote when local <> remote -> 
            raise <| IncompatibleVersionException(localVersion, remoteVersion)
        | false, _ -> 
            raise <| IncompatibleVersionException(localVersion, remoteVersion)
        | _ -> ()