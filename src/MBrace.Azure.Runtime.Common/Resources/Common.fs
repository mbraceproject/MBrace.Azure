namespace Nessos.MBrace.Azure.Runtime.Common

// Contains types used a table storage entities, service bus messages and blog objects.
open System
open System.Runtime.Serialization
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging
open Nessos.MBrace.Azure.Runtime


type IResource = 
    inherit ISerializable
    //inherit IDisposable
    abstract Uri : Uri 

module internal TableEntityUtils =     
    let PayloadSizePerProperty = 64L * 1024L
    let NumberOfProperties = 15L
    let MaxPayloadSize = NumberOfProperties * PayloadSizePerProperty

    let partitionIn n (a : byte []) =
        let n = ((float a.Length) / (float n) |> ceil |> int)
        [| for i in 0 .. n - 1 ->
            let i, j = a.Length * i / n, a.Length * (i + 1) / n
            Array.sub a i (j - i) |]

//
// Table storage entities
//
// Parameterless public ctor is needed.

type CounterEntity(name : string, value : int) = 
    inherit TableEntity(name, String.Empty)
    member val Value = value with get, set
    new () = new CounterEntity(null, 0)

type LatchEntity(name : string, value : int, size : int) = 
    inherit CounterEntity(name, value)
    member val Size = size with get, set
    new () = new LatchEntity(null, -1, -1)

type LightCellEntity(name : string, uri : string) =
    inherit TableEntity(name, String.Empty)
    member val Uri = uri with get, set
    new () = LightCellEntity(null, null)

type ResultAggregatorEntity(name : string, index : int, bloburi : string) = 
    inherit TableEntity(name, string index)
    member val Index = index with get, set
    member val Uri = bloburi with get, set
    new () = new ResultAggregatorEntity(null, -1, null)

type CancellationTokenSourceEntity(name : string, link : string) =
    inherit TableEntity(name, String.Empty)
    member val IsCancellationRequested = false with get, set
    member val Link = link with get, set
    new () = new CancellationTokenSourceEntity(null, null)

/// A lightweight object for low latency communication with the azure storage.
/// Lightweight : payload size up to 15 * 64K = 960K.
/// See 'http://www.windowsazure.com/en-us/develop/net/how-to-guides/table-services/'
/// WARNING : See the above link for any restrictions such as having a parameterless ctor,
/// and public properties.
[<AllowNullLiteral>]
type FatEntity (pk, rk, binary) =
    inherit TableEntity(pk, rk)

    let check (a : byte [] []) i = 
        let i = i - 1
        if a = null then null elif i >= a.Length then Array.empty else a.[i]
    let binaries = 
        if binary <> null 
        then TableEntityUtils.partitionIn TableEntityUtils.PayloadSizePerProperty binary
        else null

    /// Max size 64KB
    member val Item01 = check binaries 1  with get, set
    member val Item02 = check binaries 2  with get, set
    member val Item03 = check binaries 3  with get, set
    member val Item04 = check binaries 4  with get, set
    member val Item05 = check binaries 5  with get, set
    member val Item06 = check binaries 6  with get, set
    member val Item07 = check binaries 7  with get, set
    member val Item08 = check binaries 8  with get, set
    member val Item09 = check binaries 9  with get, set
    member val Item10 = check binaries 10 with get, set
    member val Item11 = check binaries 11 with get, set
    member val Item12 = check binaries 12 with get, set
    member val Item13 = check binaries 13 with get, set
    member val Item14 = check binaries 14 with get, set
    member val Item15 = check binaries 15 with get, set

    member this.GetPayload () = 
        [| this.Item01; this.Item02; this.Item03; this.Item04; this.Item05; this.Item06; this.Item07; this.Item08; this.Item09; 
           this.Item10; this.Item11; this.Item12; this.Item13; this.Item14; this.Item15; |]
        |> Array.map (fun a -> if a = null then Array.empty else a)
        |> Array.concat
        
    new () = FatEntity (null, null, null)


module Table =
    let PreconditionFailed (e : exn) =
        match e with
        | :? AggregateException as e ->
            let e = e.InnerException
            e :? StorageException && (e :?> StorageException).RequestInformation.HttpStatusCode = 412 
        | _ -> false

    let private exec<'U> config table op : Async<obj> = 
        async {
            let t = ConfigurationRegistry.Resolve<ClientProvider>(config).TableClient.GetTableReference(table)
            let! _ = t.CreateIfNotExistsAsync()
            let! (e : TableResult) = t.ExecuteAsync(op)
            return e.Result 
        }

    let insert<'T when 'T :> ITableEntity> config table (e : 'T) : Async<unit> = 
        TableOperation.Insert(e) |> exec config table |> Async.Ignore

    let insertBatch<'T when 'T :> ITableEntity> config table (e : seq<'T>) : Async<unit> =
        async {
            let batch = new TableBatchOperation()
            e |> Seq.iter batch.Insert
            let t = ConfigurationRegistry.Resolve<ClientProvider>(config).TableClient.GetTableReference(table)
            let! _ = t.CreateIfNotExistsAsync()
            let! es = t.ExecuteBatchAsync(batch)
            return ()
        }

    let insertOrReplace<'T when 'T :> ITableEntity> config table (e : 'T) : Async<unit> = 
        TableOperation.InsertOrReplace(e) |> exec config table |> Async.Ignore
    
    let read<'T when 'T :> ITableEntity> config table pk rk : Async<'T> = 
        async { 
            let t = ConfigurationRegistry.Resolve<ClientProvider>(config).TableClient.GetTableReference(table)
            let! (e : TableResult) = t.ExecuteAsync(TableOperation.Retrieve<'T>(pk, rk))
            return e.Result :?> 'T
        }
    
    let queryPK<'T when 'T : (new : unit -> 'T) and 'T :> ITableEntity> config table pk : Async<'T seq> = 
        async {  
            let t = ConfigurationRegistry.Resolve<ClientProvider>(config).TableClient.GetTableReference(table)
            let q = TableQuery<'T>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk))
            return t.ExecuteQuery<'T>(q)
        }
    
    let merge<'T when 'T :> ITableEntity> config table (e : 'T) : Async<'T> = 
        TableOperation.Merge(e) |> exec config table |> Async.Cast
    
    let replace<'T when 'T :> ITableEntity> config table (e : 'T) : Async<'T> = 
        TableOperation.Replace(e) |> exec config table |> Async.Cast

    let delete<'T when 'T :> ITableEntity> config table (e : 'T) : Async<unit> =
        TableOperation.Delete(e) |> exec config table |> Async.Ignore




(*
    //http://msdn.microsoft.com/en-us/library/azure/dd179338.aspx

    let generate arity =
        let sb = new System.Text.StringBuilder()
        let generics = {1..arity} |> Seq.map (sprintf "'T%02d") |> String.concat ", "
        let args = {1..arity} |> Seq.map(fun i -> sprintf "item%02d : 'T%02d" i i) |> String.concat ", "
        let param = {1..arity} |> Seq.map (sprintf "Unchecked.defaultof<'T%02d>") |> String.concat ", "
        Printf.bprintf sb "type TupleEntity<%s> (pk : string, rk : string, %s) =\n" generics args
        Printf.bprintf sb "    inherit TableEntity(pk, rk)\n"
        Printf.bprintf sb "    member val Item%02d = item%02d with get, set\n" |> fun f -> Seq.iter (fun i -> f i i) {1..arity}
        Printf.bprintf sb "    new () = TupleEntity<%s>(null, null,%s)\n" generics param
        sb.ToString()

    for i = 1 to 16 do
        printfn "%s" <| generate i
*)
(*
module DynamicEntity =

    let create<'T> pk rk (value : 'T) serialize =
        let e = new DynamicTableEntity(pk, rk)
        let prop =
            match box value with
            | :? bool           as value -> Some <| EntityProperty.GeneratePropertyForBool(new Nullable<_>(value))
            | :? DateTime       as value -> Some <| EntityProperty.GeneratePropertyForDateTimeOffset(new Nullable<_>(new DateTimeOffset(value)))
            | :? DateTimeOffset as value -> Some <| EntityProperty.GeneratePropertyForDateTimeOffset(new Nullable<_>(value))
            | :? double         as value -> Some <| EntityProperty.GeneratePropertyForDouble(new Nullable<_>(value))
            | :? Guid           as value -> Some <| EntityProperty.GeneratePropertyForGuid(new Nullable<_>(value))
            | :? int            as value -> Some <| EntityProperty.GeneratePropertyForInt(new Nullable<_>(value))
            | :? int64          as value -> Some <| EntityProperty.GeneratePropertyForLong(new Nullable<_>(value))
            | :? string         as value -> Some <| EntityProperty.GeneratePropertyForString(value)
            | _ -> None
        match prop with
        | Some p ->
            e.Properties.Add("Value", p)
        | None ->
            let binary = serialize value
            
            let binaries = 
                if binary = null then null
                else partitionIn PayloadSizePerProperty binary
            
            binaries 
            |> Array.iteri (fun i b -> e.Properties.Add(sprintf "Item%0d" (i + 1), EntityProperty.GeneratePropertyForByteArray(b)))
        e
*)