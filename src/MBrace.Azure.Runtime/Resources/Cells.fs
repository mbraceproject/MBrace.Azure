namespace Nessos.MBrace.Azure.Runtime.Resources

open System
open System.Runtime.Serialization
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Common
open Nessos.MBrace.Azure.Runtime.Config

type BlobCell<'T> internal (res : Uri) = 
    member __.GetValue() : Async<'T> = 
        async { 
            let container = ClientProvider.BlobClient.GetContainerReference(res.Container)
            use! s = container.GetBlockBlobReference(res.FileWithScheme).OpenReadAsync()
            return Config.serializer.Deserialize<'T>(s) 
        }
    
    interface IResource with member __.Uri = res
    
    static member Init(res : Uri, f : unit -> 'T) = 
        async { 
            let c = ClientProvider.BlobClient.GetContainerReference(res.Container)
            let! _ = c.CreateIfNotExistsAsync()
            use! s = c.GetBlockBlobReference(res.FileWithScheme).OpenWriteAsync()
            Config.serializer.Serialize<'T>(s, f())
            return new BlobCell<'T>(res)
        }
    
    static member Get<'Τ>(res : Uri) = new BlobCell<'T>(res)

    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, context: StreamingContext): unit = 
            info.AddValue("uri", res, typeof<Uri>)

    new(info: SerializationInfo, context: StreamingContext) =
        let res = info.GetValue("uri", typeof<Uri>) :?> Uri
        new BlobCell<'T>(res)

type BlobCell =
    static member GetUri(container, id) = uri "blobcell:%s/%s" container id
    static member GetUri(container) = BlobCell.GetUri(container, guid())



//type TableCell<'Τ> internal (res : Uri) = 
//    
//    member __.GetValue() : Async<'T> = 
//        async { 
//            let! e = Table.read<LightCellEntity> res.Table res.PartitionKey ""
//            let bc = BlobCell.Get(e.Uri)
//            return! bc.GetValue<'T>()
//        }
//    
//    interface IResource with member __.Uri = res
//
//    static member Init<'T>(res : Uri, f : unit -> 'T) = 
//        async { 
//            let res' = BlobCell.GetUri(res.Container)
//            let! bc = BlobCell.Init<'T>(res', f)
//            let e = new LightCellEntity(res.PartitionKey, res)
//            do! Table.insert res.Table e
//            return new TableCell<'Τ>(res)
//        }
//    
//    static member Get<'Τ>(res : Uri) = new TableCell<'Τ>(res)
//
//    interface ISerializable with
//        member x.GetObjectData(info: SerializationInfo, context: StreamingContext): unit = 
//            info.AddValue("uri", res, typeof<Uri>)
//
//    new(info: SerializationInfo, context: StreamingContext) =
//        let res = info.GetValue("uri", typeof<Uri>) :?> Uri
//        new TableCell<'T>(res)
//
//type TableCell =
//    static member GetUri(container, id) = uri "tablecell:%s/%s" container id
//    static member GetUri(container) = TableCell.GetUri(container, guid())