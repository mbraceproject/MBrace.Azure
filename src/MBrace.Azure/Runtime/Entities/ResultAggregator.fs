namespace MBrace.Azure.Runtime

open System
open System.Runtime.Serialization
open Microsoft.WindowsAzure.Storage.Table
open MBrace.Core.Internals
open MBrace.Azure
open MBrace.Azure.Runtime.Utilities
open MBrace.Runtime
open MBrace.Runtime.Components

(*
 * A ResultAggregator of length N consists of N indexed entries and 1 guard entry.
 * All entries share the ResultAggregators partitionKey, each one of the indexed use
 * its index as a rowKey, the guard entry uses an empty rowKey and acts like a counter.
 * Each update changes both the indexed entry and the control entry in a single batch operation.
 *
 *  +-------------+--------------+---------------+---------------+
 *    PartitionKey     RowKey         Uri             Counter
 *  +-------------+--------------+---------------+---------------+
 *     aggr_pk          empty            -                1
 *  +-------------+--------------+---------------+---------------+
 *     aggr_pk            0           empty              -
 *     aggr_pk            1           someUri            -
 *  +-------------+--------------+---------------+---------------+
 *
 *)

type IndexedReferenceEntity(partitionKey, rowKey) = 
    inherit TableEntity(partitionKey, rowKey)
    member val Uri = null : string with get, set
    member val WorkerId = null : string with get, set
    member val Counter = Nullable<int>() with get, set
    new() = IndexedReferenceEntity(null, null)
    static member MakeRowKey(index) = sprintf "%010d" index
    static member DefaultRowKey = String.Empty

[<DataContract; Sealed>]
type ResultAggregator<'T> internal (clusterId : ClusterId, partitionKey : string, size : int) =
    static let enableOverWrite = false
    let [<DataMember(Name = "ClusterId")>] clusterId = clusterId
    let [<DataMember(Name = "PartitionKey")>] partitionKey = partitionKey
    let [<DataMember(Name = "Size")>] size = size

    let getEntity index = async {
        let pkFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey)
        let rkFilter = 
            TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, IndexedReferenceEntity.DefaultRowKey),
                TableOperators.Or,
                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, IndexedReferenceEntity.MakeRowKey(index)))

        let query = TableQuery<IndexedReferenceEntity>().Where(TableQuery.CombineFilters(pkFilter, TableOperators.And, rkFilter))

        let! entities = Table.query<IndexedReferenceEntity> clusterId.StorageAccount clusterId.RuntimeTable query
        let guard = entities |> Seq.find (fun e -> e.RowKey = IndexedReferenceEntity.DefaultRowKey)
        let entity = entities |> Seq.find (fun e -> e.RowKey = IndexedReferenceEntity.MakeRowKey(index))
        return guard, entity
    }

    member this.UUID = partitionKey

    interface ICloudResultAggregator<'T> with
        member this.Capacity: int = size
        
        member this.CurrentSize: Async<int> = async {
            let! record = Table.read<IndexedReferenceEntity> clusterId.StorageAccount clusterId.RuntimeTable partitionKey IndexedReferenceEntity.DefaultRowKey
            return record.Counter.Value
        }
        
        member this.Dispose(): Async<unit> = async {
            let! records = Table.queryPK<IndexedReferenceEntity> clusterId.StorageAccount clusterId.RuntimeTable partitionKey
            let indexed = records |> Seq.filter (fun e -> e.RowKey <> IndexedReferenceEntity.DefaultRowKey)
            do! Table.deleteBatch clusterId.StorageAccount clusterId.RuntimeTable records
            do! 
                indexed
                |> Seq.choose (fun r -> match r.Uri with null -> None | uri -> Some uri)
                |> Seq.map (fun uri -> BlobPersist.DeletePersistedClosure(clusterId, uri))
                |> Async.Parallel
                |> Async.Ignore
        }
        
        member this.IsCompleted: Async<bool> = async {
            let! currentSize = (this :> ICloudResultAggregator<'T>).CurrentSize
            return currentSize = size
        }
        
        member this.SetResult(index: int, value: 'T, workerId : IWorkerId): Async<bool> = async { 
            let uri = sprintf "%s/%s" partitionKey <| IndexedReferenceEntity.MakeRowKey index
            do! BlobPersist.PersistClosure(clusterId, value, uri, allowNewSifts = false)
            let rec loop(guard : IndexedReferenceEntity, record : IndexedReferenceEntity) = async {
                if enableOverWrite = false && record.Uri <> null then
                    return guard.Counter.Value = size
                else
                    if record.Uri = null then
                        guard.Counter <- nullable(guard.Counter.Value + 1)

                    record.WorkerId <- workerId.Id
                    record.Uri <- uri
                    try
                        do! Table.mergeBatch clusterId.StorageAccount clusterId.RuntimeTable [guard; record]
                        return guard.Counter.Value = size
                    with ex when StoreException.PreconditionFailed ex ->
                        let! xs = getEntity index
                        return! loop xs
            }
            let! xs = getEntity index
            return! loop xs
        }
        
        member this.ToArray(): Async<'T []> = async { 
            let! records = Table.queryPK<IndexedReferenceEntity> clusterId.StorageAccount clusterId.RuntimeTable partitionKey
            let records = records |> Seq.sortBy (fun r -> r.RowKey)
            let guard = Seq.head records
            let entities = Seq.skip 1 records

            if guard.Counter.Value <> size then
                return! Async.Raise <| new InvalidOperationException(sprintf "Result aggregator incomplete (%d/%d)." guard.Counter.Value size)
            else
                return!
                    entities
                    |> Seq.map (fun e -> BlobPersist.ReadPersistedClosure<'T>(clusterId, e.Uri))
                    |> Async.Parallel
        }

[<Sealed; AutoSerializable(false)>]
type TableResultAggregatorFactory private (clusterId : ClusterId) =
    interface ICloudResultAggregatorFactory with
        member x.CreateResultAggregator(_aggregatorId : string, capacity: int): Async<ICloudResultAggregator<'T>> = async {
            let partitionKey = guid()
            let entities = seq {
                yield new IndexedReferenceEntity(partitionKey, IndexedReferenceEntity.DefaultRowKey, Counter = nullable 0)
                for i = 0 to capacity - 1 do
                    let rowKey = IndexedReferenceEntity.MakeRowKey(i)
                    yield new IndexedReferenceEntity(partitionKey, rowKey)
            }
            do! Table.insertBatch clusterId.StorageAccount clusterId.RuntimeTable entities
            return new ResultAggregator<'T>(clusterId, partitionKey, capacity) :> ICloudResultAggregator<'T>
        }
    
    static member Create(clusterId : ClusterId) = new TableResultAggregatorFactory(clusterId)