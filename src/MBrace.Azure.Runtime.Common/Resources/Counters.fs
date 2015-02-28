namespace MBrace.Azure.Runtime.Resources

// Contains types used a table storage entities, service bus messages and blog objects.
open System
open System.Runtime.Serialization
open MBrace.Continuation
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common
open MBrace.Azure

type IntCell internal (config : ConfigurationId, pk, rk) = 
    let table = config.RuntimeTable
    member __.Value = 
        let e = Table.read<CounterEntity> config table pk rk |> Async.RunSync
        e.Value
    
    member internal __.Update(updatef : int -> int) = 
        async { 
            let! e = Table.transact<CounterEntity> config table pk rk (fun e -> e.Value <- updatef e.Value)
            return e.Value
        }
    
    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, _: StreamingContext): unit = 
            info.AddValue("config", config, typeof<ConfigurationId>)
            info.AddValue("pk", pk, typeof<string>)
            info.AddValue("rk", rk, typeof<string>)

    new(info: SerializationInfo, _: StreamingContext) =
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        let pk = info.GetValue("pk", typeof<string>) :?> string
        let rk = info.GetValue("rk", typeof<string>) :?> string
        new IntCell(config, pk, rk)

    static member Create(config, name : string, value : int, pid) = 
        async { 
            let e = new CounterEntity(pid, name, value)
            do! Table.insert config config.RuntimeTable e
            return new IntCell(config, pid, name)
        }


type Latch internal (config, pk, rk) = 
    inherit IntCell(config, pk, rk)

    member __.Decrement() = base.Update(fun v -> v - 1)

    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, _: StreamingContext): unit = 
            info.AddValue("config", config, typeof<ConfigurationId>)
            info.AddValue("pk", pk, typeof<string>)
            info.AddValue("rk", rk, typeof<string>)

    new(info: SerializationInfo, _: StreamingContext) =
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        let pk = info.GetValue("pk", typeof<string>) :?> string
        let rk = info.GetValue("rk", typeof<string>) :?> string
        new Latch(config, pk, rk)

    static member Create(config, name : string, value : int, pid) = 
        async { 
            let e = new LatchEntity(pid, name, value, value)
            do! Table.insert config config.RuntimeTable e
            return new Latch(config, pid, name)
        }
    static member Create(config, value : int, pid) = 
        Latch.Create(config, guid(), value, pid)

type Counter internal (config, pk, rk) = 
    inherit IntCell(config, pk, rk)

    member __.Increment() = base.Update(fun v -> v + 1)

    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, _: StreamingContext): unit = 
            info.AddValue("config", config, typeof<ConfigurationId>)
            info.AddValue("pk", pk, typeof<string>)
            info.AddValue("rk", rk, typeof<string>)

    new(info: SerializationInfo, _: StreamingContext) =
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        let pk = info.GetValue("pk", typeof<string>) :?> string
        let rk = info.GetValue("rk", typeof<string>) :?> string
        new Counter(config, pk, rk)

    static member Create(config, name : string, value : int, pid) = 
        async { 
            let e = new CounterEntity(pid, name, value)
            do! Table.insert config config.RuntimeTable e
            return new Counter(config, pid, name)
        }
    static member Create(config, value : int, pid) = 
        Counter.Create(config, guid(), value, pid)
