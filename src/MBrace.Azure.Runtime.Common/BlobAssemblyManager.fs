namespace MBrace.Azure.Runtime.Resources

open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common
open MBrace.Azure.Runtime.Resources
open Nessos.Vagabond
open System
open System.Runtime.Serialization
open MBrace.Runtime.Vagabond
open MBrace.Azure
open MBrace.Runtime

type BlobAssemblyManager private (config : ConfigurationId, logger : ICloudLogger) = 

    // temporary fix; avoid multiple uploadings for dynamic assemblies
    // this is still wrong; need IRemoteAssemblyReceiver implementation for uploading to blob store
    let partialAssemblies = new System.Collections.Concurrent.ConcurrentDictionary<AssemblyId, bool> ()
    let isPartialDynamicAssembly (pkg : AssemblyPackage) =
        match pkg.StaticInitializer with
        | None -> false
        // is partially evaluated slice; record occurrence and return true
        | Some init when init.IsPartial -> partialAssemblies.[pkg.Id] <- true ; true
        
        | Some _  ->
            let ok, wasPartial = partialAssemblies.TryGetValue pkg.Id
            // is fully evaluated slice for the first time; record occurence and return true
            if ok && wasPartial then
                partialAssemblies.[pkg.Id] <- false ; true
            // fully evaluated slice is already uploaded
            else
                false
    
    let filename id = sprintf "%s-%s" id.FullName (Convert.toBase32String id.ImageHash) 
    let prefix = "assemblies"
    let uploadPkg (pkg : AssemblyPackage) = 
        async {
            let file =  filename pkg.Id
            let! exists = Blob<_>.Exists(config, prefix, file)
            if not exists || isPartialDynamicAssembly pkg then
                let imgSize = 
                    match pkg.Image, pkg.StaticInitializer with
                    | Some img, Some init -> sprintf "[IL %d bytes, Data %d bytes]" img.Length init.Data.Length
                    | Some img, None      -> sprintf "[IL %d bytes]" img.Length
                    | None, Some init     -> sprintf "[Data %d bytes]" init.Data.Length
                    | _                   -> String.Empty
                logger.Logf "Uploading file %s %s" pkg.FullName imgSize
                do! Blob.Create(config, prefix, file, fun () -> pkg) |> Async.Ignore
                logger.Logf "File %s done." pkg.FullName
            else
                logger.Logf "File %s exists." pkg.FullName
        }
    
    let downloadPkg (id : AssemblyId) : Async<AssemblyPackage> = 
        async { 
            let file = filename id
            logger.Logf "Downloading file %s" id.FullName
            let blob = Blob.FromPath(config, prefix, file)
            let! value = blob.GetValue()
            logger.Logf "File %s done" id.FullName
            return value
        }
    
    member __.UploadDependencies(ids : AssemblyId list) = 
        async { 
            let pkgs = VagabondRegistry.Instance.CreateAssemblyPackages(ids, includeAssemblyImage = true)
            logger.Logf "Uploading dependencies"
            do! pkgs
                |> Seq.map uploadPkg
                |> Async.Parallel
                |> Async.Ignore
        }
    
    member __.LoadDependencies(ids : AssemblyId list) = 
        async { 
            let publisher = 
                { new IRemoteAssemblyPublisher with
                      member __.GetRequiredAssemblyInfo() = async.Return ids
                      member __.PullAssemblies ids = 
                          async { 
                              let! pkgs = ids
                                          |> Seq.map downloadPkg
                                          |> Async.Parallel
                              return pkgs |> Seq.toList
                          } }
            logger.Log "Downloading dependencies."
            do! VagabondRegistry.Instance.ReceiveDependencies publisher
        }
    
    member __.ComputeDependencies(graph : 'T) = 
        VagabondRegistry.Instance.ComputeObjectDependencies(graph, permitCompilation = true) 
        |> List.map Utilities.ComputeAssemblyId

    static member Create(config, logger) = 
        new BlobAssemblyManager(config, logger)

    