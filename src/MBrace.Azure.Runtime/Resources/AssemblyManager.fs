namespace Nessos.MBrace.Azure.Runtime.Resources

open System
open System.Threading
open System.Runtime.Serialization
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Common
open Nessos.Vagrant
open Nessos.MBrace.Runtime
open Nessos.Thespian
open Nessos.Thespian.Remote.Protocols

/// Actor publication utilities
type Actor private () =
    static do Config.initRuntimeState()

    /// Publishes an actor instance to the default TCP protocol
    static member Publish(actor : Actor<'T>) =
        let name = Guid.NewGuid().ToString()
        actor
        |> Actor.rename name
        |> Actor.publish [ Protocols.utcp() ]
        |> Actor.start

    /// Exception-safe stateful actor behavior combinator
    static member Stateful (init : 'State) f = 
        let rec aux state (self : Actor<'T>) = async {
            let! msg = self.Receive()
            let! state' = async { 
                try return! f state msg 
                with e -> printfn "Actor fault (%O): %O" typeof<'T> e ; return state
            }

            return! aux state' self
        }

        Actor.bind (aux init)

    /// Exception-safe stateless actor behavior combinator
    static member Stateless (f : 'T -> Async<unit>) =
        Actor.Stateful () (fun () t -> f t)

type private AssemblyExporterMsg =
    | RequestAssemblies of AssemblyId list * IReplyChannel<AssemblyPackage list> 

/// Provides assembly uploading facility for Vagrant.
type AssemblyExporter private (exporter : ActorRef<AssemblyExporterMsg>) =
    static member Init() =
        let behaviour (RequestAssemblies(ids, ch)) = async {
            let packages = VagrantRegistry.Vagrant.CreateAssemblyPackages(ids, includeAssemblyImage = true)
            do! ch.Reply packages
        }

        let ref = 
            Actor.Stateless behaviour
            |> Actor.Publish
            |> Actor.ref

        new AssemblyExporter(ref)

    /// <summary>
    ///     Request the loading of assembly dependencies from remote
    ///     assembly exporter to the local application domain.
    /// </summary>
    /// <param name="ids">Assembly id's to be loaded in app domain.</param>
    member __.LoadDependencies(ids : AssemblyId list) = async {
        let publisher =
            {
                new IRemoteAssemblyPublisher with
                    member __.GetRequiredAssemblyInfo () = async { return ids }
                    member __.PullAssemblies ids = exporter <!- fun ch -> RequestAssemblies(ids, ch)
            }

        do! VagrantRegistry.Vagrant.ReceiveDependencies publisher
    }

    /// <summary>
    ///     Compute assembly dependencies for provided object graph.
    /// </summary>
    /// <param name="graph">Object graph to be analyzed</param>
    member __.ComputeDependencies (graph:'T) =
        VagrantRegistry.Vagrant.ComputeObjectDependencies(graph, permitCompilation = true)
        |> List.map Utilities.ComputeAssemblyId


//type AssemblyExporter private (res : Uri) =
//
//
//    member __.LoadDependencies(ids : AssemblyId list) = async {
//        let publisher =
//            {
//                new IRemoteAssemblyPublisher with
//                    member __.GetRequiredAssemblyInfo () = async { return ids }
//                    member __.PullAssemblies ids = 
//                        async { return VagrantRegistry.Vagrant.CreateAssemblyPackages(ids, includeAssemblyImage = true) }
//            }
//
//        do! VagrantRegistry.Vagrant.ReceiveDependencies publisher
//    }
//
//    member __.ComputeDependencies (graph:'T) =
//        VagrantRegistry.Vagrant.ComputeObjectDependencies(graph, permitCompilation = true)
//        |> List.map Utilities.ComputeAssemblyId
//
//
//    static member Get(res : Uri) = new AssemblyExporter(res)
//    static member GetUri(container, id) = uri "exporter:%s/%s" container id
//    static member GetUri(container) = AssemblyExporter.GetUri(container, guid())
//    static member Init(res : Uri) = new AssemblyExporter(res)
//
//    //temporary
//    static member Init() = new AssemblyExporter(AssemblyExporter.GetUri "tmp")
//
//
//
//    interface IResource with member __.Uri = res
//
//    interface ISerializable with
//        member x.GetObjectData(info: SerializationInfo, context: StreamingContext): unit = 
//            info.AddValue("uri", res, typeof<Uri>)
//
//    new(info: SerializationInfo, context: StreamingContext) =
//        let res = info.GetValue("uri", typeof<Uri>) :?> Uri
//        new AssemblyExporter(res)