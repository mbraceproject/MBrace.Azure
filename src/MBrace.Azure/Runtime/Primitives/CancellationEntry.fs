namespace MBrace.Azure.Runtime

open System
open System.Collections.Generic
open System.Threading
open System.Runtime.Serialization

open Microsoft.WindowsAzure.Storage.Table

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities
open MBrace.Runtime
open MBrace.Runtime

// Implements an Azure-table based ICloudCancellationEntry:
// an entity that can be canceled and which supports child entities.
// Used to implement CancellationTokens in MBrace

/// CancellationToken table entity; Supports up to 5120 children
type CancellationTokenSourceEntity(uuid : string, children : seq<string>) =
    inherit TableEntity(CancellationTokenSourceEntity.DefaultPartitionKey, uuid)

    let groupings = Seq.chunksOf 512 children |> Array.map (String.concat ";")
    do if groupings.Length > 10 then 
        raise <| ArgumentOutOfRangeException("Number of cancellation entry children exceeds maximum permitted.")

    let getGrouping i =
        if i >= groupings.Length then null else groupings.[i]

    member val IsCancellationRequested = false with get, set

    member val Children0 : string = getGrouping 0 with get, set
    member val Children1 : string = getGrouping 1 with get, set
    member val Children2 : string = getGrouping 2 with get, set
    member val Children3 : string = getGrouping 3 with get, set
    member val Children4 : string = getGrouping 4 with get, set
    member val Children5 : string = getGrouping 5 with get, set
    member val Children6 : string = getGrouping 6 with get, set
    member val Children7 : string = getGrouping 7 with get, set
    member val Children8 : string = getGrouping 8 with get, set
    member val Children9 : string = getGrouping 9 with get, set

    member this.GetChildren () =
        [| this.Children0; this.Children1; this.Children2; this.Children3; this.Children4 
           this.Children5; this.Children6; this.Children7; this.Children8; this.Children9 |]
        |> Seq.collect(fun c -> if c = null then [||] else c.Split(';'))
        |> Seq.toList
        
    new () = new CancellationTokenSourceEntity(null, [||])

    static member DefaultPartitionKey = "cancellationToken"
    static member MaxChildrenPerProperty = 512

[<Sealed>]
type internal TableCancellationEntry (config : ClusterId, uuid : string) =
    interface ICancellationEntry with        
        member x.UUID: string = uuid

        member x.Cancel(): Async<unit> = async {
            let visited = new HashSet<string>()
            let rec walk rowKey = async {
                if not <| visited.Contains rowKey then
                    let! e = Table.read<CancellationTokenSourceEntity> config.StorageAccount config.RuntimeTable CancellationTokenSourceEntity.DefaultPartitionKey rowKey
                    if e.IsCancellationRequested then ()
                    else
                        let _ = visited.Add rowKey
                        for e' in e.GetChildren() do do! walk e'
            }

            do! walk uuid
        
            do! visited
                |> Seq.map (fun rowKey -> new CancellationTokenSourceEntity(rowKey, [||], IsCancellationRequested = true, ETag = "*"))
                |> Table.mergeBatch config.StorageAccount config.RuntimeTable
        }
        
        member x.Dispose(): Async<unit> = async {
            do! Table.delete config.StorageAccount config.RuntimeTable (new CancellationTokenSourceEntity(uuid, [||]))
        }
        
        member x.IsCancellationRequested: Async<bool> = async {
            let! record = Table.read<CancellationTokenSourceEntity> config.StorageAccount config.RuntimeTable CancellationTokenSourceEntity.DefaultPartitionKey uuid
            return record.IsCancellationRequested
        }
        

[<Sealed>]
type TableCancellationTokenFactory private (config : ClusterId) =
    interface ICancellationEntryFactory with
        member x.CreateCancellationEntry(): Async<ICancellationEntry> = async {
            let record = new CancellationTokenSourceEntity(guid(), [||])
            let! _record = Table.insert config.StorageAccount config.RuntimeTable record
            return new TableCancellationEntry(config, record.RowKey) :> ICancellationEntry
        }
        
        member x.TryCreateLinkedCancellationEntry(parents: ICancellationEntry []): Async<ICancellationEntry option> = async {
            let uuid = guid()
            let record = new CancellationTokenSourceEntity(uuid, [||])

            let rec loop () = async {
                let! parents = 
                    parents 
                    |> Seq.map (fun p -> Table.read<CancellationTokenSourceEntity> config.StorageAccount config.RuntimeTable CancellationTokenSourceEntity.DefaultPartitionKey p.UUID)
                    |> Async.Parallel

                if parents |> Array.exists (fun p -> p.IsCancellationRequested) then
                    return None
                else
                    let tbo = TableBatchOperation()
                    tbo.Insert(record)
                    for parent in parents do
                        let newParent = new CancellationTokenSourceEntity(parent.RowKey, uuid :: parent.GetChildren())
                        newParent.ETag <- parent.ETag
                        tbo.Merge(newParent)
                    try
                        do! Table.batch config.StorageAccount config.RuntimeTable tbo
                        return Some(TableCancellationEntry(config, uuid) :> ICancellationEntry)
                    with ex when StoreException.PreconditionFailed ex ->
                        return! loop ()
            }

            return! loop ()
        }
        
    static member Create(config : ClusterId) = 
        new TableCancellationTokenFactory(config)