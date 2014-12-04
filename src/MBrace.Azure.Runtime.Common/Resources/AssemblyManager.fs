namespace Nessos.MBrace.Azure.Runtime.Resources

open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Common
open Nessos.MBrace.Azure.Runtime.Resources
open Nessos.MBrace.Runtime
open Nessos.Vagrant
open System
open System.Runtime.Serialization
open System.Threading
open Nessos.MBrace.Azure.Runtime.Common.Storage
open Nessos.MBrace.Runtime.Vagrant

type AssemblyManager private (config : ConfigurationId, res : Uri) = 
    
    let uploadPkg (pkg : AssemblyPackage) = 
        async { 
            return! BlobCell.CreateIfNotExists(config, res.Container, pkg.FullName, fun () -> pkg) |> Async.Ignore
        }
    
    let downloadPkg (id : AssemblyId) : Async<AssemblyPackage> = 
        async { 
            let uri = BlobCell<_>.GetUri(res.Container, id.FullName)
            let cell = BlobCell.OfUri(config, uri)
            return! cell.GetValue()
        }
    
    member __.UploadDependencies(ids : AssemblyId list) = 
        async { 
            let pkgs = VagrantRegistry.Vagrant.CreateAssemblyPackages(ids, includeAssemblyImage = true)
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
            do! VagrantRegistry.Vagrant.ReceiveDependencies publisher
        }
    
    member __.ComputeDependencies(graph : 'T) = 
        VagrantRegistry.Vagrant.ComputeObjectDependencies(graph, permitCompilation = true) 
        |> List.map Utilities.ComputeAssemblyId

    interface ISerializable with
        member x.GetObjectData(info : SerializationInfo, context : StreamingContext) : unit = 
            info.AddValue("uri", res, typeof<Uri>)
            info.AddValue("config", config, typeof<ConfigurationId>)
    
    new(info : SerializationInfo, context : StreamingContext) = 
        let res = info.GetValue("uri", typeof<Uri>) :?> Uri
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new AssemblyManager(config, res)

    static member private GetUri(container) = uri "exporter:%s" container
    static member Create(config, container : string) = 
        let res = AssemblyManager.GetUri(container)
        new AssemblyManager(config, res)

    