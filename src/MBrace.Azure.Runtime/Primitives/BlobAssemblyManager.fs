namespace MBrace.Azure.Runtime.Primitives

open System
open System.IO
open System.Text.RegularExpressions
open System.Runtime.Serialization

open Nessos.Vagabond
open Nessos.Vagabond.AssemblyProtocols

open MBrace.Runtime
open MBrace.Runtime.Vagabond
open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities

#nowarn "1571"

[<AutoOpen>]
module private Common =

    /// blob prefix for stored assemblies
    let prefix = "vagabond"

    /// Gets a unique blob filename for provided assembly
    let filename (id : AssemblyId) = Vagabond.GetFileName id

    let assemblyName id = filename id + ".dll"
    let symbolsName id = filename id + ".pdb"
    let metadataName id = filename id + ".vmetadata"
    let staticdataName gen id = sprintf "%s-%d.vdata" (filename id) gen
        

/// Assembly to blob store uploader implementation
type private BlobAssemblyUploader(config : ConfigurationId, logger : ICloudLogger) =

    let getAssemblyLoadInfo (id : AssemblyId) = async {
        let! assemblyExists = Blob.Exists(config, prefix, assemblyName id)
        if not assemblyExists then return NotLoaded id
        else
            let cell = Blob<VagabondMetadata>.FromPath(config, prefix, metadataName id)
            let! metadata = cell.TryGetValue()
            return Loaded(id, false, metadata)
    }

    /// upload assembly to blob store
    let uploadAssembly (va : VagabondAssembly) = async {
        let assemblyName = assemblyName va.Id
        let! assemblyExists = Blob.Exists(config, prefix, assemblyName)

        /// print upload sizes for given assembly
        let uploadSizes = seq {
            let size (path:string) = FileInfo(path).Length |> getHumanReadableByteSize
            if not assemblyExists then
                yield sprintf "IL %s" (size va.Image)
                match va.Symbols with
                | Some s -> yield sprintf "PDB %s" (size s)
                | None -> ()

            match va.Metadata with
            | Some (_,d) -> yield sprintf "Data %s" (size d)
            | None -> ()
        }

        logger.Logf "Uploading assembly '%s' [%s]" va.FullName <| String.concat ", " uploadSizes

        if not assemblyExists then
            let! _ = Blob.UploadFromFile(config, prefix, assemblyName, va.Image)
            return ()

        match va.Symbols with
        | None -> ()
        | Some symbolsPath ->
            let symbolsName = symbolsName va.Id
            let! symbolsExist = Blob.Exists(config, prefix, symbolsName)
            if not symbolsExist then
                let! _ = Blob.UploadFromFile(config, prefix, symbolsName, symbolsPath)
                return ()

        match va.Metadata with
        | None -> return Loaded(va.Id, false, None)
        | Some (metadata, dataPath) ->
            let dataName = staticdataName metadata.Generation va.Id
            let! dataExists = Blob.Exists(config, prefix, dataName)
            if not dataExists then
                let! _ = Blob.UploadFromFile(config, prefix, dataName, dataPath)
                return ()

            let! _ = Blob<VagabondMetadata>.Create(config, prefix, metadataName va.Id, fun () -> metadata)
            return Loaded(va.Id, false, Some metadata)
    }

    interface IRemoteAssemblyReceiver with
        member x.GetLoadedAssemblyInfo(dependencies: AssemblyId list): Async<AssemblyLoadInfo list> = async {
            let! loadInfo = dependencies |> Seq.map getAssemblyLoadInfo |> Async.Parallel
            return Array.toList loadInfo
        }
        
        member x.PushAssemblies(assemblies: VagabondAssembly list): Async<AssemblyLoadInfo list> =  async {
            let! loadInfo = assemblies |> Seq.map uploadAssembly |> Async.Parallel
            return Array.toList loadInfo
        }


/// Blob store assembly downloader implementation
type private BlobAssemblyDownloader(config : ConfigurationId, logger : ICloudLogger) =
    
    interface IAssemblyImporter with
        member x.GetImageReader(id: AssemblyId): Async<Stream> = async {
            logger.Logf "Downloading assembly '%s'" id.FullName
            let blob = Blob.FromPath(config, prefix, assemblyName id)
            return! blob.OpenRead()
        }
        
        member x.TryGetSymbolReader(id: AssemblyId): Async<Stream option> = async {
            let! exists = Blob.Exists(config, prefix, symbolsName id)
            if exists then
                let blob = Blob.FromPath(config, prefix, symbolsName id)
                let! stream = blob.OpenRead()
                return Some stream
            else
                return None
        }
        
        member x.TryReadMetadata(id: AssemblyId): Async<VagabondMetadata option> = async {
            let cell = Blob<VagabondMetadata>.FromPath(config, prefix, metadataName id)
            return! cell.TryGetValue()
        }

        member x.GetDataReader(id: AssemblyId, vmd : VagabondMetadata): Async<Stream> = async {
            logger.Logf "Downloading vagabond data for assembly '%s'." id.FullName
            let blob = Blob.FromPath(config, prefix, staticdataName vmd.Generation id)
            return! blob.OpenRead()
        }

/// Assembly manager instance
type BlobAssemblyManager private (config : ConfigurationId, logger : ICloudLogger, includeUnmanagedDependencies : bool) = 
    let uploader = new BlobAssemblyUploader(config, logger)
    let downloader = new BlobAssemblyDownloader(config, logger)

    /// Upload provided dependencies to store
    member __.UploadDependencies(ids : seq<AssemblyId>) = async { 
        logger.Logf "Uploading dependencies"
        let! errors = VagabondRegistry.Instance.SubmitAssemblies(uploader, ids)
        if errors.Length > 0 then
            let errors = errors |> Seq.map (fun (f,_) -> f.ToString()) |> String.concat ", "
            logger.Logf "Failed to upload bindings: %s" errors
    }

    /// Download provided dependencies from store
    member __.DownloadDependencies(ids : seq<AssemblyId>) = async {
        return! VagabondRegistry.Instance.ImportAssemblies(downloader, ids)
    }

    /// Load local assemblies to current AppDomain
    member __.LoadAssemblies(assemblies : VagabondAssembly list) =
        VagabondRegistry.Instance.LoadVagabondAssemblies(assemblies)

    /// Compute dependencies for provided object graph
    member __.ComputeDependencies(graph : 'T) : AssemblyId list =
        let managedDependencies = VagabondRegistry.Instance.ComputeObjectDependencies(graph, permitCompilation = true) 
        [
            yield! managedDependencies |> VagabondRegistry.Instance.GetVagabondAssemblies
            if includeUnmanagedDependencies then 
                yield! VagabondRegistry.Instance.NativeDependencies
        ] |> List.map (fun va -> va.Id)

    /// Creates a new AssemblyManager instance for given store configuration
    static member Create(config : ConfigurationId, logger : ICloudLogger, ?includeUnmanagedAssemblies : bool) : BlobAssemblyManager = 
        new BlobAssemblyManager(config, logger, defaultArg includeUnmanagedAssemblies true)

    /// <summary>
    ///     Registers a native assembly dependency to client state.
    /// </summary>
    /// <param name="assemblyPath">Path to native assembly.</param>
    static member RegisterNativeDependency(assemblyPath : string) : VagabondAssembly =
        VagabondRegistry.Instance.RegisterNativeDependency assemblyPath

    /// Gets all native dependencies registered in current instance
    static member NativeDependencies =
        VagabondRegistry.Instance.NativeDependencies |> List.map (fun m -> m.Image)