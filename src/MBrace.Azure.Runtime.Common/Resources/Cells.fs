namespace MBrace.Azure.Runtime.Resources

open System
open System.Runtime.Serialization
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common
open System.IO
open MBrace.Azure
open Microsoft.WindowsAzure.Storage.Blob
open Microsoft.WindowsAzure.Storage

[<DataContract>]
type Blob<'T> internal (config : ConfigurationId, prefix : string, filename : string) =
    
    [<DataMember(Name = "config")>]
    let config = config
    [<DataMember(Name = "prefix")>]
    let prefix = prefix
    [<DataMember(Name = "filename")>]
    let filename = filename

    member __.GetValue() : Async<'T> = 
        async { 
            let container = ConfigurationRegistry.Resolve<StoreClientProvider>(config).BlobClient.GetContainerReference(config.RuntimeContainer)
            use! s = container.GetBlockBlobReference(sprintf "%s/%s" prefix filename).OpenReadAsync()
            return Configuration.Pickler.Deserialize<'T>(s) 
        }
    
    member __.Path = sprintf "%s/%s" prefix filename

    static member FromPath(config : ConfigurationId, path : string) = 
        let p = path.Split('/')
        Blob<'T>.FromPath(config, p.[0], p.[1])

    static member FromPath(config : ConfigurationId, prefix, file) = 
        new Blob<'T>(config, prefix, file)

    static member Exists(config, prefix, filename) =
        async {
            let c = ConfigurationRegistry.Resolve<StoreClientProvider>(config).BlobClient.GetContainerReference(config.RuntimeContainer)
            let! _ = c.CreateIfNotExistsAsync()
            let b = c.GetBlockBlobReference(sprintf "%s/%s" prefix filename)
            return! b.ExistsAsync()
        }

    static member Create(config, prefix, filename : string, f : unit -> 'T) = 
        async { 
            let c = ConfigurationRegistry.Resolve<StoreClientProvider>(config).BlobClient.GetContainerReference(config.RuntimeContainer)
            let! _ = c.CreateIfNotExistsAsync()
            let b = c.GetBlockBlobReference(sprintf "%s/%s" prefix filename)

            let options = BlobRequestOptions(ServerTimeout = Nullable<_>(TimeSpan.FromMinutes(40.)))
            use! stream = b.OpenWriteAsync(null, options, OperationContext(), Async.DefaultCancellationToken)
            Configuration.Pickler.Serialize<'T>(stream, f())
            do! stream.FlushAsync()
            stream.Dispose()

            // For some reason large client uploads, fail to upload but do not throw exception...
            let! exists = b.ExistsAsync()
            if not exists then failwith(sprintf "Failed to upload %s/%s" prefix filename)

            return new Blob<'T>(config, prefix, filename)
        }