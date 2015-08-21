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
    member val Counter = value with get, set
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
                let! e = Table.transact<CounterEntity> config config.RuntimeTable partitionKey CounterEntity.DefaultRowKey (fun e -> e.Counter <- 1 + e.Counter)
                return e.Counter
            }

        member x.Value: Async<int> = 
            async {
                let! e = Table.read<CounterEntity> config config.RuntimeTable partitionKey CounterEntity.DefaultRowKey
                return e.Counter
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