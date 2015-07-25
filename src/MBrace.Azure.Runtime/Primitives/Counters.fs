namespace MBrace.Azure.Runtime

open System
open System.Runtime.Serialization
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities
open MBrace.Azure
open Microsoft.WindowsAzure.Storage.Table
open MBrace.Runtime

// NOTE : All types that inherit TableEntity must provide a default public ctor.
type CounterEntity(id : string, value : int) = 
    inherit TableEntity(id, CounterEntity.DefaultRowKey)
    member val Value = value with get, set
    new () = new CounterEntity(null, 0)
    static member DefaultRowKey = String.Empty

[<DataContract; Sealed>]
type internal Int32Counter (config : ConfigurationId, partitionKey : string) =
    let [<DataMember(Name = "config")>] config = config    
    let [<DataMember(Name = "partitionKey")>] partitionKey = partitionKey

    interface ICloudCounter with
        member x.Dispose(): Async<unit> = 
            async {
                do! Table.delete config config.RuntimeTable <| CounterEntity(PartitionKey = partitionKey)
            }
        
        member x.Increment(): Async<int> = 
            async { 
                let! e = Table.transact<CounterEntity> config config.RuntimeTable partitionKey CounterEntity.DefaultRowKey (fun e -> e.Value <- 1 + e.Value)
                return e.Value
            }

        member x.Value: Async<int> = 
            async {
                let! e = Table.read<CounterEntity> config config.RuntimeTable partitionKey CounterEntity.DefaultRowKey
                return e.Value
            }

[<Sealed>]
type Int32CounterFactory private (config : ConfigurationId) =
    interface ICloudCounterFactory with
        member x.CreateCounter(initialValue: int): Async<ICloudCounter> = 
            async {
                let record = new CounterEntity(guid(), initialValue)
                let! _record = Table.insert config config.RuntimeTable record
                return new Int32Counter(config, record.PartitionKey) :> ICloudCounter
            }

    static member Create(config) = new Int32CounterFactory(config) :> ICloudCounterFactory