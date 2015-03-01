namespace MBrace.Azure.Runtime.Resources

open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common
open MBrace.Azure.Runtime.Resources
open Nessos.Vagabond
open System
open System.Runtime.Serialization
open MBrace.Runtime.Vagabond
open MBrace.Azure

type AssemblyManager private (config : ConfigurationId) = 
    
    let filename id = sprintf "%s-%s" id.FullName (Convert.toBase32String id.ImageHash)
    
    let uploadPkg (pkg : AssemblyPackage) = 
        async { 
            return! Blob.CreateIfNotExists(config, "assemblies", filename pkg.Id, fun () -> pkg) |> Async.Ignore
        }
    
    let downloadPkg (id : AssemblyId) : Async<AssemblyPackage> = 
        async { 
            let blob = Blob.FromPath(config, "assemblies", filename id)
            return! blob.GetValue()
        }
    
    member __.UploadDependencies(ids : AssemblyId list) = 
        async { 
            let pkgs = VagabondRegistry.Instance.CreateAssemblyPackages(ids, includeAssemblyImage = true)
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
            do! VagabondRegistry.Instance.ReceiveDependencies publisher
        }
    
    member __.ComputeDependencies(graph : 'T) = 
        VagabondRegistry.Instance.ComputeObjectDependencies(graph, permitCompilation = true) 
        |> List.map Utilities.ComputeAssemblyId

    interface ISerializable with
        member x.GetObjectData(info : SerializationInfo, _ : StreamingContext) : unit = 
            info.AddValue("config", config, typeof<ConfigurationId>)
    
    new(info : SerializationInfo, _ : StreamingContext) = 
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new AssemblyManager(config)

    static member Create(config) = 
        new AssemblyManager(config)

    