namespace MBrace.Azure.Runtime.Resources

open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common
open MBrace.Azure.Runtime.Resources
open Nessos.Vagabond
open System
open System.Runtime.Serialization
open MBrace.Runtime.Vagabond

type AssemblyManager private (config : ConfigurationId, res : Uri) = 
    
    let filename id = sprintf "%s-%s" id.FullName (Convert.toBase32String id.ImageHash)
    
    let uploadPkg (pkg : AssemblyPackage) = 
        async { 
            return! BlobCell.CreateIfNotExists(config, res.Primary, filename pkg.Id, fun () -> pkg) |> Async.Ignore
        }
    
    let downloadPkg (id : AssemblyId) : Async<AssemblyPackage> = 
        async { 
            let uri = BlobCell<_>.GetUri(res.Primary, filename id)
            let cell = BlobCell.OfUri(config, uri)
            return! cell.GetValue()
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
            info.AddValue("uri", res, typeof<Uri>)
            info.AddValue("config", config, typeof<ConfigurationId>)
    
    new(info : SerializationInfo, _ : StreamingContext) = 
        let res = info.GetValue("uri", typeof<Uri>) :?> Uri
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new AssemblyManager(config, res)

    static member private GetUri(container) = uri "exporter:%s" container
    static member Create(config, container : string) = 
        let res = AssemblyManager.GetUri(container)
        new AssemblyManager(config, res)

    