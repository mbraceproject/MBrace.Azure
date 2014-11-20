namespace Nessos.MBrace.Azure.Runtime

open System
open System.Reflection
open System.Threading

open Nessos.Vagrant

open Nessos.MBrace.Runtime
open Nessos.FsPickler
open Microsoft.WindowsAzure.Storage
open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging

type Configuration = 
    { StorageConnectionString : string
      ServiceBusConnectionString : string }

    static member Serializer = VagrantRegistry.Pickler

    static member Initialize(config : Configuration) =
        let runOnce (f : unit -> 'T) = let v = lazy(f ()) in fun () -> v.Value
        runOnce(fun () ->
            let _ = System.Threading.ThreadPool.SetMinThreads(100, 100)

            // vagrant initialization
            let ignoredAssemblies =
                let this = Assembly.GetExecutingAssembly()
                let dependencies = Utilities.ComputeAssemblyDependencies(this, requireLoadedInAppDomain = false)
                new System.Collections.Generic.HashSet<_>(dependencies)

            VagrantRegistry.Initialize(ignoreAssembly = ignoredAssemblies.Contains, loadPolicy = AssemblyLoadPolicy.ResolveAll)) ()
        ClientProvider.Activate config

and [<AbstractClass; Sealed>] ClientProvider private () = 
    static let cfg = ref None
    static let acc = ref Unchecked.defaultof<CloudStorageAccount>
    
    static let check f = 
        lock cfg (fun () -> 
            if cfg.Value.IsNone then failwith "No active configuration found."
            else f())
    
    static member Activate(config : Configuration) = 
        let sa = CloudStorageAccount.Parse(config.StorageConnectionString)
        lock cfg (fun () -> 
            cfg := Some config
            acc := sa)
    
    static member ActiveConfiguration = check (fun _ -> cfg.Value.Value)
    static member TableClient = check (fun _ -> acc.Value.CreateCloudTableClient())
    static member BlobClient = check (fun _ -> acc.Value.CreateCloudBlobClient())
    static member NamespaceClient = 
        check (fun _ -> NamespaceManager.CreateFromConnectionString(cfg.Value.Value.ServiceBusConnectionString))
    static member QueueClient(queue : string) = 
        check (fun _ -> QueueClient.CreateFromConnectionString(cfg.Value.Value.ServiceBusConnectionString, queue))

