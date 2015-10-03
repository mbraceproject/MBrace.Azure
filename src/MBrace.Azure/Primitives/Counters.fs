namespace MBrace.Azure.Runtime

open System
open System.Runtime.Serialization
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities
open MBrace.Azure
open Microsoft.WindowsAzure.Storage.Table
open MBrace.Runtime

// Defines a distributed counter for use by the MBrace execution engine
// using Azure table storage

// NOTE : All types that inherit TableEntity must provide a default public ctor.
type CounterEntity(id : string, value : int64) = 
    inherit TableEntity(id, CounterEntity.DefaultRowKey)
    member val Counter = value with get, set
    new () = new CounterEntity(null, 0L)
    static member DefaultRowKey = String.Empty

[<DataContract; Sealed>]
type internal TableCounter (config : ClusterId, partitionKey : string) =
    let [<DataMember(Name = "config")>] config = config
    let [<DataMember(Name = "partitionKey")>] partitionKey = partitionKey

    interface ICloudCounter with
        member x.Dispose(): Async<unit> = async {
            do! Table.delete config.StorageAccount config.RuntimeTable <| CounterEntity(PartitionKey = partitionKey)
        }
        
        member x.Increment(): Async<int64> = async { 
            let! e = Table.transact<CounterEntity> config.StorageAccount config.RuntimeTable partitionKey CounterEntity.DefaultRowKey (fun e -> e.Counter <- 1L + e.Counter)
            return e.Counter
        }

        member x.Value: Async<int64> = async {
            let! e = Table.read<CounterEntity> config.StorageAccount config.RuntimeTable partitionKey CounterEntity.DefaultRowKey
            return e.Counter
        }

[<Sealed>]
type TableCounterFactory private (config : ClusterId) =
    interface ICloudCounterFactory with
        member x.CreateCounter(initialValue: int64): Async<ICloudCounter> = async {
            let record = new CounterEntity(guid(), initialValue)
            let! _record = Table.insert config.StorageAccount config.RuntimeTable record
            return new TableCounter(config, record.PartitionKey) :> ICloudCounter
        }

    static member Create(config : ClusterId) = new TableCounterFactory(config)