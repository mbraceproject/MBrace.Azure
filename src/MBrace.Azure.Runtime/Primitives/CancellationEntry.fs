namespace MBrace.Azure.Runtime.Primitives

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

// Stores around 20K CTS links :
// 1 link = 32 chars (guid without hyphen)
// 10 properties * 64KB => 20K links
type CancellationTokenSourceEntity(uuid : string, links : string seq) =
    inherit TableEntity(CancellationTokenSourceEntity.DefaultPartitionKey, uuid)

    let split =
        if box links <> null then
            let s = 
                links 
                |> String.concat ";"
            let chunkSize = 64 * 1024 // 64KB per property
            {0..chunkSize..s.Length-1}
            |> Seq.map(fun i -> s.Substring(i, Math.Min(chunkSize, s.Length-i)))
            |> Seq.toArray
        else null

    let check (a : string []) i =
        if a = null then null else if i >= a.Length then String.Empty else a.[i]

    member val IsCancellationRequested = false with get, set

    member val Link0 = check split 0 with get, set
    member val Link1 = check split 1 with get, set
    member val Link2 = check split 2 with get, set
    member val Link3 = check split 3 with get, set
    member val Link4 = check split 4 with get, set
    member val Link5 = check split 5 with get, set
    member val Link6 = check split 6 with get, set
    member val Link7 = check split 7 with get, set
    member val Link8 = check split 8 with get, set
    member val Link9 = check split 9 with get, set

    member this.GetLinks () =
        String.Concat(
            this.Link0, this.Link1, this.Link2, this.Link3, this.Link4, 
            this.Link5, this.Link6, this.Link7, this.Link8, this.Link9).Split(';')
        |> Seq.filter(not << String.IsNullOrEmpty)
        |> Seq.toList
        
    new () = new CancellationTokenSourceEntity(null, null)

    static member DefaultPartitionKey = "ctoken"

[<Sealed>]
type internal CancellationEntry (config : ConfigurationId, uuid) =
    interface ICancellationEntry with
        member x.Cancel(): Async<unit> = 
            async {
                let visited = new HashSet<string>()
                let rec walk rowKey = async {
                    if not <| visited.Contains rowKey then
                        let! e = Table.read<CancellationTokenSourceEntity> config config.RuntimeTable CancellationTokenSourceEntity.DefaultPartitionKey rowKey
                        if e.IsCancellationRequested then ()
                        else
                            let _ = visited.Add rowKey
                            for e' in e.GetLinks() do do! walk e'
                }

                do! walk uuid
        
                do! visited
                    |> Seq.map (fun rowKey -> new CancellationTokenSourceEntity(rowKey, null, IsCancellationRequested = true, ETag = "*"))
                    |> Table.mergeBatch config config.RuntimeTable
            }
        
        member x.Dispose(): Async<unit> = 
            async {
                do! Table.delete config config.RuntimeTable (new CancellationTokenSourceEntity(uuid, null))
            }
        
        member x.IsCancellationRequested: Async<bool> = 
            async {
                let! record = Table.read<CancellationTokenSourceEntity> config config.RuntimeTable CancellationTokenSourceEntity.DefaultPartitionKey uuid
                return record.IsCancellationRequested
            }
        
        member x.UUID: string = uuid
        

[<Sealed>]
type CancellationTokenFactory private (config : ConfigurationId) =
    interface ICancellationEntryFactory with
        member x.CreateCancellationEntry(): Async<ICancellationEntry> = 
            async {
                let record = new CancellationTokenSourceEntity(guid(), List.empty)
                let! _record = Table.insert config config.RuntimeTable record
                return new CancellationEntry(config, record.PartitionKey) :> ICancellationEntry
            }
        
        member x.TryCreateLinkedCancellationEntry(parents: ICancellationEntry []): Async<ICancellationEntry option> = 
            async {
                let uuid = guid()
                let record = new CancellationTokenSourceEntity(uuid, List.Empty)

                let rec loop _ = async {
                    let! parents = 
                        parents 
                        |> Seq.map (fun p -> Table.read<CancellationTokenSourceEntity> config config.RuntimeTable CancellationTokenSourceEntity.DefaultPartitionKey p.UUID)
                        |> Async.Parallel
                    if parents |> Seq.forall (fun p -> p.IsCancellationRequested) then
                        return None
                    else
                        let tbo = TableBatchOperation()
                        tbo.Insert(record)
                        for parent in parents do
                            let newParent = new CancellationTokenSourceEntity(parent.RowKey, uuid :: parent.GetLinks())
                            newParent.ETag <- parent.ETag
                            tbo.Merge(newParent)
                        try
                            do! Table.batch config config.RuntimeTable tbo
                            return Some(CancellationEntry(config, uuid) :> ICancellationEntry)
                        with ex when Table.PreconditionFailed ex ->
                            return! loop ()
                }
                return! loop ()
            }
        
    static member Create(config : ConfigurationId) = new CancellationTokenFactory(config) :> ICancellationEntryFactory