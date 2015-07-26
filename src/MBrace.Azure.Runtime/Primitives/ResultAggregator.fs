namespace MBrace.Azure.Runtime

open System
open System.Runtime.Serialization
open Microsoft.WindowsAzure.Storage.Table
open MBrace.Core.Internals
open MBrace.Azure
open MBrace.Azure.Runtime.Utilities
open MBrace.Runtime

type BlobReferenceEntity(partitionKey, rowKey, uri : string) = 
    inherit TableEntity(partitionKey, rowKey)
    member val Uri = uri with get, set
    new() = BlobReferenceEntity(null, null, null)
    static member internal MakeRowKey(rowKey, index) = sprintf "%s:%010d" rowKey index


[<DataContract; Sealed>]
type ResultAggregator<'T> internal (config : ConfigurationId, partitionKey : string, rowKey : string, size : int) = 

    [<DataMember(Name = "config")>]
    let config = config
    [<DataMember(Name = "partitionKey")>]
    let partitionKey = partitionKey
    [<DataMember(Name = "rowKey")>]
    let rowKey = rowKey
    [<DataMember(Name = "size")>]
    let size = size

    let getEntities () = async {
        let pkFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey)
        let lower = BlobReferenceEntity.MakeRowKey(rowKey, 0)
        let upper = BlobReferenceEntity.MakeRowKey(rowKey, size)
        let rkFilter = 
            TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, upper),
                TableOperators.And,
                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, lower))

        let query = TableQuery<BlobReferenceEntity>().Where(TableQuery.CombineFilters(pkFilter, TableOperators.And, rkFilter))

        let! xs = Table.query<BlobReferenceEntity> config config.RuntimeTable query
        return xs
    }

    let completed () =
        async {
            let! xs = getEntities()
            return xs |> Seq.forall (fun e -> not <| String.IsNullOrEmpty(e.Uri))
        }

    interface ICloudResultAggregator<'T> with
        member this.Capacity: int = size
        
        member this.CurrentSize: Async<int> = 
            async {
                let! entities = getEntities()
                return entities |> Seq.filter (fun e -> e.Uri <> null) |> Seq.length
            }
        
        member this.Dispose(): Async<unit> = 
            async {
                let! entities = getEntities()
                do! entities 
                    |> Seq.map (fun e -> async { if e.Uri <> null then return! Blob.Delete(config, e.Uri) })
                    |> Async.Parallel
                    |> Async.Ignore
                do! Table.deleteBatch config config.RuntimeTable entities
            }
        
        member this.IsCompleted: Async<bool> = completed()
        
        member this.SetResult(index: int, value: 'T, overwrite: bool): Async<bool> = 
            async { 
                let! bc = Blob.Create(config, partitionKey, guid(), fun () -> value)
                if overwrite then
                    let e = new BlobReferenceEntity(partitionKey, BlobReferenceEntity.MakeRowKey(rowKey, index), null, ETag = "*")
                    e.Uri <- bc.Path
                    let! _ = Table.merge config config.RuntimeTable e
                    return! completed()
                else
                    let! _ = Table.transact<BlobReferenceEntity> config config.RuntimeTable partitionKey (BlobReferenceEntity.MakeRowKey(rowKey, index))
                                (fun e ->
                                    if e.Uri = null then e.Uri <- bc.Path)
                    return! completed()
            }
        
        member this.ToArray(): Async<'T []> = 
            async { 
                let! entities = getEntities()

                if entities |> Seq.exists (fun e -> e.Uri = null) then
                    return! Async.Raise <| new InvalidOperationException("Result aggregator incomplete.")
                else
                    let bs = 
                        entities
                        |> Seq.sortBy (fun x -> x.RowKey)
                        |> Seq.map (fun x -> x.Uri)
                        |> Seq.map (fun x -> Blob<'T>.FromPath(config, x))
                        |> Seq.toArray

                    let re = Array.zeroCreate<'T> bs.Length
                    let i = ref 0
                    for b in bs do
                        let! v = b.GetValue()
                        re.[!i] <- v
                        incr i
                    return re
            }

[<Sealed>]
type ResultAggregatorFactory private (config : ConfigurationId) =
    interface ICloudResultAggregatorFactory with
        member x.CreateResultAggregator(capacity: int): Async<ICloudResultAggregator<'T>> = 
            async {
                let key = guid()
                let entities = seq {
                    for i = 0 to capacity - 1 do
                        let rowKey = BlobReferenceEntity.MakeRowKey(key, i)
                        yield new BlobReferenceEntity(key, rowKey, null)
                }
                do! Table.insertBatch config config.RuntimeTable entities
                return new ResultAggregator<'T>(config, key, key, capacity) :> ICloudResultAggregator<'T>
            }
    
    static member Create(config) = new ResultAggregatorFactory(config) :> ICloudResultAggregatorFactory