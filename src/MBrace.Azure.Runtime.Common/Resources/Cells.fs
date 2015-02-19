namespace MBrace.Azure.Runtime.Resources

open System
open System.Runtime.Serialization
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common


type BlobCell<'T> internal (config : ConfigurationId, res : Uri) = 
    member __.GetValue() : Async<'T> = 
        async { 
            let container = ConfigurationRegistry.Resolve<ClientProvider>(config).BlobClient.GetContainerReference(res.Primary)
            use! s = container.GetBlockBlobReference(res.SecondaryWithScheme).OpenReadAsync()
            return Configuration.Pickler.Deserialize<'T>(s) 
        }
    
    member __.Uri = res
    
    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, context: StreamingContext): unit = 
            info.AddValue("uri", res, typeof<Uri>)
            info.AddValue("config", config, typeof<ConfigurationId>)

    new(info: SerializationInfo, context: StreamingContext) =
        let res = info.GetValue("uri", typeof<Uri>) :?> Uri
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new BlobCell<'T>(config, res)

    static member OfUri<'T>(config, res : Uri) = new BlobCell<'T>(config, res)
    static member GetUri(container, id) = uri "blobcell:%s/%s" container id
    static member CreateIfNotExists(config, container : string, id : string, f : unit -> 'T) = 
        async { 
            let res = BlobCell<_>.GetUri(container, id)
            let c = ConfigurationRegistry.Resolve<ClientProvider>(config).BlobClient.GetContainerReference(res.Primary)
            let! _ = c.CreateIfNotExistsAsync()
            let b = c.GetBlockBlobReference(res.SecondaryWithScheme)
            let! exists = b.ExistsAsync()
            if not exists then
                use! s = b.OpenWriteAsync()
                Configuration.Pickler.Serialize<'T>(s, f())
                return new BlobCell<'T>(config, res)
            else
                return BlobCell.OfUri<'T>(config, res)
        }
    static member CreateIfNotExists(config, container : string, f : unit -> 'T) = 
        BlobCell.CreateIfNotExists(config, container, guid(), f)