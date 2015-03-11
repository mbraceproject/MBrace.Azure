namespace MBrace.Azure.Runtime.Primitives

// Contains types used a table storage entities, service bus messages and blog objects.
open System
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open MBrace.Azure.Runtime

//
// Table storage entities
//
// Parameterless public ctor is needed.
[<AutoOpen>]
module private Helper =
    let empty = Unchecked.defaultof<string>

[<AbstractClass>]
/// Base class for all process-bound runtime resources.
type RuntimeEntity (pid, rk, entity) =
    inherit TableEntity(pid, rk)
    member val EntityType = entity with get, set
    
type CounterEntity(pid, name : string, value : int) = 
    inherit RuntimeEntity(pid, name, "CNT")
    member val Value = value with get, set
    new () = new CounterEntity(empty, empty, 0)

//type LatchEntity(pid, name : string, value : int, size : int) = 
//    inherit CounterEntity(pid, name, value)
//    member val Size = size with get, set
//    new () = new LatchEntity(empty, empty, -1, -1)

type BlobReferenceEntity(pid, name : string, uri : string) =
    inherit RuntimeEntity(pid, name, "REF")
    member val Uri = uri with get, set
    new () = BlobReferenceEntity(empty, empty, empty)
    new(pid, name) = new BlobReferenceEntity(pid, name, empty)

// Stores around 10K CTS links.
type CancellationTokenSourceEntity(pid, id : string, links : (string * string) list) =
    inherit RuntimeEntity(pid, id, "CTS")

    let split =
        if box links <> null then
            let s = 
                links 
                |> Seq.map (fun (pk, rk) -> sprintf "%s/%s" pk rk)
                |> String.concat ";"
            let chunkSize = 64 * 1024 // 64KB per property
            {0..chunkSize..s.Length-1}
            |> Seq.map(fun i -> s.Substring(i, Math.Min(chunkSize, s.Length-i)))
            |> Seq.toArray
        else null

    let check (a : string []) i =
        if a = null then null else if i >= a.Length then String.Empty else a.[i]

    member val IsCancellationRequested = false with get, set
    member val Metadata = empty with get, set

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
        |> Seq.map (fun pkrk -> let xs = pkrk.Split('/') in xs.[0], xs.[1])
        |> Seq.toList
        
    new () = new CancellationTokenSourceEntity(empty, empty, Unchecked.defaultof<_>)

// Used to create batch operations on multiple resource requests.
type TableResourceOperation =
    abstract Operations : TableOperation seq
type TableResourceOperation<'TResource> =
    inherit TableResourceOperation
    abstract Resource : 'TResource