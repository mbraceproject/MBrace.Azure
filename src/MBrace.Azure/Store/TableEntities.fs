namespace MBrace.Azure.Store.TableEntities

open System
open System.IO
open System.Threading.Tasks

open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table

open MBrace.Azure.Runtime.Utilities
open MBrace.Azure.Store

module internal TableEntityConfig =

    let PayloadSizePerProperty = 64 * 1024
    let NumberOfProperties = 15
    let MaxPayloadSize = NumberOfProperties * PayloadSizePerProperty

/// A lightweight object for low latency communication with the azure storage.
/// Lightweight : payload size up to 15 * 64K = 960K.
/// See 'http://www.windowsazure.com/en-us/develop/net/how-to-guides/table-services/'
/// WARNING : See the above link for any restrictions such as having a parameterless ctor,
/// and public properties.
[<AllowNullLiteral>]
type FatEntity (partitionKey : string, rowKey : string, blob : byte []) =
    inherit TableEntity(partitionKey, rowKey)

    let chunks = 
        if blob = null then null
        elif blob.Length > TableEntityConfig.MaxPayloadSize then
            invalidOp <| sprintf "Blob payload exceeds limit of %d bytes." TableEntityConfig.MaxPayloadSize
        else 
            Array.chunksOf TableEntityConfig.PayloadSizePerProperty blob

    let item i = 
        let i = i - 1
        if chunks = null then null
        elif i >= chunks.Length then Array.empty
        else
            chunks.[i]

    /// Max size 64KB
    member val Item01 = item 1  with get, set
    member val Item02 = item 2  with get, set
    member val Item03 = item 3  with get, set
    member val Item04 = item 4  with get, set
    member val Item05 = item 5  with get, set
    member val Item06 = item 6  with get, set
    member val Item07 = item 7  with get, set
    member val Item08 = item 8  with get, set
    member val Item09 = item 9  with get, set
    member val Item10 = item 10 with get, set
    member val Item11 = item 11 with get, set
    member val Item12 = item 12 with get, set
    member val Item13 = item 13 with get, set
    member val Item14 = item 14 with get, set
    member val Item15 = item 15 with get, set

    member this.GetPayload () =
        if this.Item01 = null then null
        else
            [| this.Item01; this.Item02; this.Item03; this.Item04; this.Item05; this.Item06; this.Item07; this.Item08; this.Item09; 
               this.Item10; this.Item11; this.Item12; this.Item13; this.Item14; this.Item15; |]
            |> Array.concat
        
    new () = FatEntity (null, null, null)


module internal Table =
    let private checkExn code (e : exn) =
        match e with
        | :? StorageException as e -> e.RequestInformation.HttpStatusCode = code
        | :? AggregateException as e ->
            let e = e.InnerException
            e :? StorageException && (e :?> StorageException).RequestInformation.HttpStatusCode = code
        | _ -> false

    
    let PreconditionFailed (e : exn) = checkExn 412 e
    let Conflict (e : exn) = checkExn 409 e
    let NotFound (e : exn) = checkExn 404 e

    let getRandomName () =
        // See http://blogs.msdn.com/b/jmstall/archive/2014/06/12/azure-storage-naming-rules.aspx
        let alpha = [|'a'..'z'|]
        let alphaNumeric = Array.append alpha [|'0'..'9'|]
        let maxLen = 63
        let randOf =
            let rand = new Random(int DateTime.Now.Ticks)
            fun (x : char []) -> x.[rand.Next(0, x.Length)]

        let name = 
            [| yield randOf alpha
               for _i = 1 to maxLen-1 do yield randOf alphaNumeric |]
        new String(name)