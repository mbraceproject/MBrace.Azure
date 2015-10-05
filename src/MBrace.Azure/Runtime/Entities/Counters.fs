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
type internal TableCounter (clusterId : ClusterId, partitionKey : string) =
    let [<DataMember(Name = "ClusterId")>] id = clusterId
    let [<DataMember(Name = "PartitionKey")>] partitionKey = partitionKey

    interface ICloudCounter with
        member x.Dispose(): Async<unit> = async {
            do! Table.delete id.StorageAccount id.RuntimeTable <| CounterEntity(PartitionKey = partitionKey)
        }
        
        member x.Increment(): Async<int64> = async { 
            let! e = Table.transact<CounterEntity> id.StorageAccount id.RuntimeTable partitionKey CounterEntity.DefaultRowKey (fun e -> e.Counter <- 1L + e.Counter)
            return e.Counter
        }

        member x.Value: Async<int64> = async {
            let! e = Table.read<CounterEntity> id.StorageAccount id.RuntimeTable partitionKey CounterEntity.DefaultRowKey
            return e.Counter
        }

[<Sealed>]
type TableCounterFactory private (clusterId : ClusterId) =
    interface ICloudCounterFactory with
        member x.CreateCounter(initialValue: int64): Async<ICloudCounter> = async {
            let record = new CounterEntity(guid(), initialValue)
            let! _record = Table.insert clusterId.StorageAccount clusterId.RuntimeTable record
            return new TableCounter(clusterId, record.PartitionKey) :> ICloudCounter
        }

    static member Create(clusterId : ClusterId) = new TableCounterFactory(clusterId)