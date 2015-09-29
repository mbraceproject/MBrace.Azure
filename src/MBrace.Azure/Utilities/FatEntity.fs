namespace MBrace.Azure.Runtime.Utilities

open System
open System.IO
open System.Threading.Tasks

open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table

open MBrace.Azure.Runtime.Utilities

module FatEntityConfiguration =

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
        elif blob.Length > FatEntityConfiguration.MaxPayloadSize then
            invalidOp <| sprintf "Blob payload exceeds limit of %d bytes." FatEntityConfiguration.MaxPayloadSize
        else 
            Array.chunksOf FatEntityConfiguration.PayloadSizePerProperty blob

    let item i = 
        let i = i - 1
        if chunks = null then null
        elif i >= chunks.Length then Array.empty
        else
            chunks.[i]

    new () = FatEntity (null, null, null)

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