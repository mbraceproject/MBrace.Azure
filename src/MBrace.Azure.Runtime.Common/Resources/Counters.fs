namespace MBrace.Azure.Runtime.Resources

// Contains types used a table storage entities, service bus messages and blog objects.
open System
open System.Runtime.Serialization
open MBrace.Continuation
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common
open MBrace.Azure

[<DataContract>]
type IntCell internal (config : ConfigurationId, partitionKey : string, rowKey : string) =

    [<DataMember(Name = "config")>]
    let config = config
    [<DataMember(Name = "partitionKey")>]
    let partitionKey = partitionKey
    [<DataMember(Name = "rowKey")>]
    let rowKey = rowKey

    member __.Value = 
        let e = Table.read<CounterEntity> config config.RuntimeTable partitionKey rowKey |> Async.RunSync
        e.Value
    
    member internal __.Update(updatef : int -> int) = 
        async { 
            let! e = Table.transact<CounterEntity> config config.RuntimeTable partitionKey rowKey (fun e -> e.Value <- updatef e.Value)
            return e.Value
        }

    static member Create(config, name : string, value : int, pid) = 
        async { 
            let e = new CounterEntity(pid, name, value)
            do! Table.insert config config.RuntimeTable e
            return new IntCell(config, pid, name)
        }

[<DataContract>]
type Latch internal (config, partitionKey, rowKey) = 
    inherit IntCell(config, partitionKey, rowKey)

    member __.Decrement() = base.Update(fun v -> v - 1)

    static member Create(config, name : string, value : int, pid) = 
        async { 
            let e = new LatchEntity(pid, name, value, value)
            do! Table.insert config config.RuntimeTable e
            return new Latch(config, pid, name)
        }

    static member Create(config, value : int, pid) = 
        Latch.Create(config, guid(), value, pid)

[<DataContract>]
type Counter internal (config, partitionKey, rowKey) = 
    inherit IntCell(config, partitionKey, rowKey)

    member __.Increment() = base.Update(fun v -> v + 1)

    static member Create(config, name : string, value : int, pid) = 
        async { 
            let e = new CounterEntity(pid, name, value)
            do! Table.insert config config.RuntimeTable e
            return new Counter(config, pid, name)
        }

    static member Create(config, value : int, pid) = 
        Counter.Create(config, guid(), value, pid)
