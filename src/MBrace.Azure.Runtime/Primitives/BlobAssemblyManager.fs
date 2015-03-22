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
        

type private BlobAssemblyUploader(config : ConfigurationId, logger : ICloudLogger) =

    let getAssemblyLoadInfo (id : AssemblyId) = async {
        let! assemblyExists = Blob.Exists(config, prefix, assemblyName id)
        if not assemblyExists then return NotLoaded id
        else
            let cell = Blob<VagabondMetadata>.FromPath(config, prefix, metadataName id)
            let! metadata = cell.TryGetValue()
            return Loaded(id, false, metadata)
    }

    let uploadAssembly (va : VagabondAssembly) = async {
        let assemblyName = assemblyName va.Id
        let! assemblyExists = Blob.Exists(config, prefix, assemblyName)

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


type BlobAssemblyManager private (config : ConfigurationId, logger : ICloudLogger, includeUnmanagedDependencies : bool) = 
    let uploader = new BlobAssemblyUploader(config, logger)
    let downloader = new BlobAssemblyDownloader(config, logger)

    member __.UploadDependencies(ids : seq<AssemblyId>) = async { 
        logger.Logf "Uploading dependencies"
        let! errors = VagabondRegistry.Instance.SubmitAssemblies(uploader, ids)
        if errors.Length > 0 then
            let errors = errors |> Seq.map (fun (f,_) -> f.ToString()) |> String.concat ", "
            logger.Logf "Failed to upload bindings: %s" errors
    }

    member __.DownloadDependencies(ids : seq<AssemblyId>) = async {
        return! VagabondRegistry.Instance.ImportAssemblies(downloader, ids)
    }

    member __.LoadAssemblies(assemblies : VagabondAssembly list) =
        VagabondRegistry.Instance.LoadVagabondAssemblies(assemblies)

    member __.ComputeDependencies(graph : 'T) =
        let managedDependencies = VagabondRegistry.Instance.ComputeObjectDependencies(graph, permitCompilation = true) 
        [
            yield! managedDependencies |> VagabondRegistry.Instance.GetVagabondAssemblies
            if includeUnmanagedDependencies then 
                yield! VagabondRegistry.Instance.UnManagedDependencies
        ] |> List.map (fun va -> va.Id)

    static member Create(config, logger, ?includeUnmanagedAssemblies) = 
        new BlobAssemblyManager(config, logger, defaultArg includeUnmanagedAssemblies true)