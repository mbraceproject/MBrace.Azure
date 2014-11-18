namespace Nessos.MBrace.Azure.Runtime.Resources

open System
open System.Runtime.Serialization
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Common


type BlobCell<'T> internal (res : Uri) = 
    member __.GetValue() : Async<'T> = 
        async { 
            let container = ClientProvider.BlobClient.GetContainerReference(res.Container)
            use! s = container.GetBlockBlobReference(res.FileWithScheme).OpenReadAsync()
            return Configuration.Serializer.Deserialize<'T>(s) 
        }
    
    interface IResource with 
        member __.Uri = res
    
    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, context: StreamingContext): unit = 
            info.AddValue("uri", res, typeof<Uri>)

    new(info: SerializationInfo, context: StreamingContext) =
        let res = info.GetValue("uri", typeof<Uri>) :?> Uri
        new BlobCell<'T>(res)

    static member OfUri<'T>(res : Uri) = new BlobCell<'T>(res)
    static member GetUri(container, id) = uri "blobcell:%s/%s" container id
    static member Init(container : string, id : string, f : unit -> 'T) = 
        async { 
            let res = BlobCell<_>.GetUri(container, id)
            let c = ClientProvider.BlobClient.GetContainerReference(res.Container)
            let! _ = c.CreateIfNotExistsAsync()
            use! s = c.GetBlockBlobReference(res.FileWithScheme).OpenWriteAsync()
            Configuration.Serializer.Serialize<'T>(s, f())
            return new BlobCell<'T>(res)
        }
    static member Init(container : string, f : unit -> 'T) = 
        BlobCell.Init(container, guid(), f)