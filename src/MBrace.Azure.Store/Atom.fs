namespace Nessos.MBrace.Azure.Store

open System
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Nessos.MBrace.Store
open Nessos.MBrace
open Nessos.MBrace.Continuation
open System.Runtime.Serialization
open System.IO

[<AutoSerializable(true) ; Sealed; DataContract>]
type Atom<'T> internal (table, pk, rk, connectionString : string, serializer : ISerializer) =
    
    [<DataMember(Name = "ConnectionString")>]
    let connectionString = connectionString
    [<DataMember(Name = "Serializer")>]
    let serializer = serializer
    [<DataMember(Name = "Table")>]
    let table = table
    [<DataMember(Name = "PartitionKey")>]
    let pk = pk
    [<DataMember(Name = "RowKey")>]
    let rk = rk

    [<IgnoreDataMember>]
    let mutable client = Table.getClient(CloudStorageAccount.Parse connectionString)

    [<OnDeserialized>]
    let onDeserialized (_ : StreamingContext) =
        client <- Table.getClient(CloudStorageAccount.Parse connectionString )

    interface ICloudAtom<'T> with

        member this.Value : 'T = Async.RunSync((this :> ICloudAtom<'T>).GetValue())
        
        member this.Id = sprintf "%s/%s/%s" table pk rk

        member this.Update(updater: 'T -> 'T, ?maxRetries : int): Async<unit> = 
            async {
                let interval = let r = new Random() in r.Next(2,10) 
                let maxInterval = 5000
                let maxRetries = defaultArg maxRetries Int32.MaxValue
                let rec update currInterval count = async {
                    if count >= maxRetries then
                        return raise <| exn("Maximum number of retries exceeded.")
                    else
                        let! e = Table.read<FatEntity> client table pk rk
                        let oldValue = unpickle(e.GetPayload()) serializer
                        let newValue = updater oldValue
                        let newBinary = pickle newValue serializer
                        let e = new FatEntity(e.PartitionKey, String.Empty, newBinary, ETag = e.ETag)
                        let! result = Async.Catch <| Table.merge client table e
                        match result with
                        | Choice1Of2 _ -> return ()
                        | Choice2Of2 e when Table.PreconditionFailed e -> 
                            do! Async.Sleep currInterval
                            return! update (min (interval * currInterval) maxInterval) (count+1)
                        | Choice2Of2 e -> return raise e
                }

                return! update interval 0
            }       
        member this.Dispose(): Async<unit> = 
            async {
                let! e = Table.read<FatEntity> client table pk rk
                return! Table.delete<FatEntity> client table e
            }
        

        member this.Force(newValue: 'T): Async<unit> = 
            async {
                let! e = Table.read<FatEntity> client table pk rk
                let newBinary = pickle newValue serializer
                let e = new FatEntity(e.PartitionKey, String.Empty, newBinary, ETag = "*")
                let! _ = Table.merge client table e
                return ()
            }

        member this.GetValue(): Async<'T> = 
            async {
                let! e = Table.read<FatEntity> client table pk rk
                let value = unpickle(e.GetPayload()) serializer
                return value
            }

///  Store implementation that uses a Azure Blob Storage as backend.
[<AbstractClass; DataContract>]
type AtomProvider (connectionString : string, serializer : ISerializer) =
    
    [<DataMember(Name = "ConnectionString")>]
    let connectionString = connectionString
    [<DataMember(Name = "Serializer")>]
    let serializer = serializer

    [<IgnoreDataMember>]
    let mutable acc = CloudStorageAccount.Parse(connectionString)
    [<IgnoreDataMember>]
    let mutable client = Table.getClient acc

    [<OnDeserialized>]
    let onDeserialized (_ : StreamingContext) =
        acc <- CloudStorageAccount.Parse(connectionString)
        client <- Table.getClient acc

    abstract ComputeSize : 'T -> int64

    interface ICloudAtomProvider with
        member this.Id = client.StorageUri.PrimaryUri.ToString()
            
        member this.Name = "Azure Table Storage Atom Provider" 

        member this.IsSupportedValue(value: 'T) : bool = 
            this.ComputeSize(value) <= TableEntityUtils.MaxPayloadSize
        
        member this.CreateUniqueContainerName() = (guid()).Substring(0,5) // TODO : Change

        member this.CreateAtom(path, initial: 'T) = 
                async {
                    let binary = pickle initial serializer
                    let e = new FatEntity(guid(), String.Empty, binary)
                    do! Table.insert<FatEntity> client path e
                    return new Atom<'T>(path, e.PartitionKey, e.RowKey, connectionString, serializer) :> ICloudAtom<'T>
                }

        member this.DisposeContainer(path) =
            async {
                do! client.GetTableReference(path).DeleteIfExistsAsync()
            }