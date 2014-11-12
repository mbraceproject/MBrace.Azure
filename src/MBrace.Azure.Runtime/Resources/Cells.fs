module Nessos.MBrace.Azure.Runtime.Cells

open System
open Nessos.MBrace.Azure.Runtime.Common

/// Read-only blob.   
type BlobCell<'T> internal (res : Uri) = 
    member __.GetValue<'T>() = 
        async { 
            let container = ClientProvider.BlobClient.GetContainerReference(res.Container)
            use! s = container.GetBlockBlobReference(res.File).OpenReadAsync()
            return Config.serializer.Deserialize<'T>(s) 
        }
    
    interface IResource with member __.Uri = res
    
    static member Init<'Τ>(res : Uri, f : unit -> 'T) = 
        async { 
            let c = ClientProvider.BlobClient.GetContainerReference(res.Container)
            let! _ = c.CreateIfNotExistsAsync()
            use! s = c.GetBlockBlobReference(res.File).OpenWriteAsync()
            Config.serializer.Serialize<'T>(s, f())
            return new BlobCell<'T>(res)
        }
    
    static member Get<'Τ>(res : Uri) = new BlobCell<'T>(res)

type BlobCell =
    static member GetUri(container, id) = uri "blobcell:%s/%s" container id
    static member GetUri(container) = BlobCell.GetUri(container, guid())



type TableCell<'Τ> internal (res : Uri) = 
    
    member __.GetValue() : Async<'T> = 
        async { 
            let! e = Table.read<LightCellEntity> res.Table res.PartitionKey ""
            let bc = BlobCell.Get(e.Uri)
            return! bc.GetValue<'T>()
        }
    
    interface IResource with member __.Uri = res

    static member Init(res : Uri, f : unit -> 'T) = 
        async { 
            let res' = BlobCell.GetUri(res.Container)
            let! bc = BlobCell.Init(res', f)
            let e = new LightCellEntity(res.PartitionKey, res)
            do! Table.insert res.Table e
            return new TableCell<'Τ>(res)
        }
    
    static member Get<'Τ>(res : Uri) = new TableCell<'Τ>(res)

type TableCell =
    static member GetUri(container, id) = uri "tablecell:%s/%s" container id
    static member GetUri(container) = TableCell.GetUri(container, guid())