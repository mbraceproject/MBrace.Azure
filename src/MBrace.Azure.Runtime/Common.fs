module Nessos.MBrace.Azure.Runtime.Common

// Contains types used a table storage entities, service bus messages and blog objects.
open System
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging

type AzureConfig = 
    { StorageConnectionString : string
      ServiceBusConnectionString : string }

type IResource = 
    abstract Uri : Uri

[<AutoOpen>]
module UriUtils = 
    let guid() = Guid.NewGuid().ToString("N")
    let uri fmt = Printf.ksprintf (fun s -> new Uri(s)) fmt
    
    let toContainerId (res : Uri) = 
        let s = res.Segments.[0]
        s.Substring(0, s.Length - 1), res.Segments.[1]

[<AbstractClass; Sealed>]
type ClientProvider private () = 
    static let cfg = ref None
    static let acc = ref Unchecked.defaultof<CloudStorageAccount>
    
    static let check f = 
        lock cfg (fun () -> 
            if cfg.Value.IsNone then failwith "No active configuration found."
            else f())
    
    static member Activate(config : AzureConfig) = 
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

// Parameterless public ctor is needed.
type LatchEntity(name : string, value : int) = 
    inherit TableEntity(name, String.Empty)
    member val Value = value with get, set
    new() = new LatchEntity(null, 0)
