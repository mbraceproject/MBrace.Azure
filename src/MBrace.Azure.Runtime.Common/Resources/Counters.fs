namespace Nessos.MBrace.Azure.Runtime.Resources

// Contains types used a table storage entities, service bus messages and blog objects.
open System
open System.Runtime.Serialization
open Microsoft.WindowsAzure.Storage
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Common
open Microsoft.WindowsAzure.Storage.Table

type IntCell internal (config : ConfigurationId, res : Uri) = 
    member __.Value = 
        let e = Table.read<CounterEntity> config res.Table res.PartitionWithScheme "" |> Async.RunSynchronously
        e.Value
    
    member internal __.Update(updatef : int -> int) = 
        async { 
            let rec update() = 
                async { 
                    let! e = Table.read<CounterEntity> config res.Table res.PartitionWithScheme ""
                    e.Value <- updatef e.Value
                    let r = ref None
                    let! result = Async.Catch <| Table.merge config res.Table e
                    match result with
                    | Choice1Of2 r -> 
                        return r.Value
                    | Choice2Of2 e when Table.PreconditionFailed e -> 
                        return! update()
                    | Choice2Of2 e -> return raise e
                }
            return! update()
        }
    
    interface IResource with
        member __.Uri = res

    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, context: StreamingContext): unit = 
            info.AddValue("uri", res, typeof<Uri>)
            info.AddValue("config", config, typeof<ConfigurationId>)

    new(info: SerializationInfo, context: StreamingContext) =
        let res = info.GetValue("uri", typeof<Uri>) :?> Uri
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new IntCell(config, res)

    static member GetUri(container, id) = uri "intcell:%s/%s" container id
    static member Create(config, container : string, value : int) = 
        async { 
            let res = IntCell.GetUri(container, guid () )
            let e = new CounterEntity(res.PartitionWithScheme, value)
            do! Table.insert config res.Table e
            return new IntCell(config, res)
        }


type Latch internal (config, res : Uri) = 
    inherit IntCell(config, res)

    member __.Decrement() = base.Update(fun v -> v - 1)

    interface IResource with 
        override __.Uri = res
    
    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, context: StreamingContext): unit = 
            info.AddValue("uri", res, typeof<Uri>)
            info.AddValue("config", config, typeof<ConfigurationId>)

    new(info: SerializationInfo, context: StreamingContext) =
        let res = info.GetValue("uri", typeof<Uri>) :?> Uri
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new Latch(config, res)

    static member private GetUri(container, id) = uri "latch:%s/%s" container id
    static member Create(config, container : string, id : string, value : int) = 
        async { 
            let res = Latch.GetUri(container, id)
            let e = new LatchEntity(res.PartitionWithScheme, value, value)
            do! Table.insert config res.Table e
            return new Latch(config, res)
        }
    static member Create(config, container : string, value : int) = 
        Latch.Create(config, container, guid(), value)

type Counter internal (config, res : Uri) = 
    inherit IntCell(config, res)
    
    member __.Increment() = base.Update(fun x -> x + 1)
    
    interface IResource with 
        override __.Uri = res

    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, context: StreamingContext): unit = 
            info.AddValue("uri", res, typeof<Uri>)
            info.AddValue("config", config, typeof<ConfigurationId>)

    new(info: SerializationInfo, context: StreamingContext) =
        let res = info.GetValue("uri", typeof<Uri>) :?> Uri
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new Counter(config, res)

    static member private GetUri(container, id) = uri "counter:%s/%s" container id
    static member Create(config, container : string, value : int) = 
        async { 
            let res = Counter.GetUri(container, guid())
            let e = new CounterEntity(res.PartitionWithScheme, value)
            do! Table.insert config res.Table e
            return new Counter(config, res)
        }