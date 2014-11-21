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
    { /// Azure storage connection string.
      StorageConnectionString : string
      /// Service Bus connection string.
      ServiceBusConnectionString : string
      /// Default Blob/Table storage container and table name
      DefaultTableOrContainer : string
      /// Default Service Bus queue name.
      DefaultQueue : string
      /// Default Table name for logging.
      DefaultLogTable : string }

    static member Default = 
        { StorageConnectionString = 
            "DefaultEndpointsProtocol=[https];AccountName=[myAccount];AccountKey=[myKey];"
          ServiceBusConnectionString = 
              "Endpoint=sb://[your namespace].servicebus.windows.net;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=[your secret]"
          DefaultTableOrContainer = "mbraceruntime"
          DefaultQueue = "mbraceruntime"
          DefaultLogTable = "mbraceruntimelogs" }

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

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Configuration =
    let private runOnce (f : unit -> 'T) = let v = lazy(f ()) in fun () -> v.Value

    let private init =
        runOnce(fun () ->
            let _ = System.Threading.ThreadPool.SetMinThreads(100, 100)

            // vagrant initialization
            let ignoredAssemblies =
                let this = Assembly.GetExecutingAssembly()
                let dependencies = Utilities.ComputeAssemblyDependencies(this, requireLoadedInAppDomain = false)
                new System.Collections.Generic.HashSet<_>(dependencies)
            let ignore assembly =
                ignoredAssemblies.Contains(assembly) || assembly.FullName.StartsWith "MBrace.Azure.Client" // TODO : change
            VagrantRegistry.Initialize(ignoreAssembly = ignoredAssemblies.Contains, loadPolicy = AssemblyLoadPolicy.ResolveAll))

    let Serializer = init () ; VagrantRegistry.Pickler

    let Initialize () = init ()
    let Activate(config : Configuration) = init (); ClientProvider.Activate config

