namespace Nessos.MBrace.Azure.Runtime.Resources

open System
open System.Threading
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Common
open Nessos.Vagrant
open Nessos.MBrace.Runtime

// Dummy implementation.

type AssemblyExporter private () =
    static member Init() = new AssemblyExporter()

    member __.LoadDependencies(ids : AssemblyId list) = async {
        let publisher =
            {
                new IRemoteAssemblyPublisher with
                    member __.GetRequiredAssemblyInfo () = async { return ids }
                    member __.PullAssemblies ids = 
                        async { return VagrantRegistry.Vagrant.CreateAssemblyPackages(ids, includeAssemblyImage = true) }
            }

        do! VagrantRegistry.Vagrant.ReceiveDependencies publisher
    }

    member __.ComputeDependencies (graph:'T) =
        VagrantRegistry.Vagrant.ComputeObjectDependencies(graph, permitCompilation = true)
        |> List.map Utilities.ComputeAssemblyId