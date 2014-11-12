namespace Nessos.MBrace.Azure.Runtime.Resources

// Contains types used a table storage entities, service bus messages and blog objects.
open System
open Microsoft.WindowsAzure.Storage
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Common

type IntCell internal (res : Uri) = 
    member __.Value = 
        let e = Table.read<CounterEntity> res.Table res.PartitionKey "" |> Async.RunSynchronously
        e.Value
    
    member internal __.Update(updatef : int -> int) = 
        async { 
            let rec update() = 
                async { 
                    let! e = Table.read<CounterEntity> res.Table res.PartitionKey ""
                    e.Value <- updatef e.Value
                    let r = ref None
                    let! result = Async.Catch <| Table.merge res.Table e
                    match result with
                    | Choice1Of2 r -> 
                        return r.Value
                    | Choice2Of2 e when Table.PreconditionFailed e -> 
                        return! update()
                    | Choice2Of2 e -> return raise e
                }
            return! update()
        }
    
    static member Init(res : Uri, value : int) = 
        async { 
            let e = new CounterEntity(res.PartitionKey, value)
            do! Table.insert res.Table e
            return new IntCell(res)
        }
    
    static member Get(res : Uri) = new IntCell(res)
    
    interface IResource with
        member __.Uri = res
    
    static member GetUri(container, id) = uri "intcell:%s/%s" container id
    static member GetUri(container) = IntCell.GetUri(container, guid())


type Latch internal (res : Uri) = 
    inherit IntCell(res)

    member __.Decrement() = base.Update(fun v -> v - 1)

    interface IResource with member __.Uri = res
    
    static member Init(res : Uri, value : int) = 
        async { 
            let e = new LatchEntity(res.PartitionKey, value, value)
            do! Table.insert res.Table e
            return new Latch(res)
        }

    static member Get(res : Uri) = new Latch(res)
    static member GetUri(container, id) = uri "latch:%s/%s" container id
    static member GetUri(container) = Latch.GetUri(container, guid())

type Counter internal (res : Uri) = 
    inherit IntCell(res)
    
    member __.Increment() = base.Update(fun x -> x + 1)
    
    interface IResource with member __.Uri = res

    static member Init(res : Uri, value : int) = 
        async { 
            let e = new CounterEntity(res.PartitionKey, value)
            do! Table.insert res.Table e
            return new Counter(res)
        }

    static member Get(res : Uri) = new Counter(res)
    static member GetUri(container, id) = uri "counter:%s/%s" container id
    static member GetUri(container) = Counter.GetUri(container, guid())
