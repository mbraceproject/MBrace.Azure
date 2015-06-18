namespace MBrace.Azure.Store

open System
open System.Runtime.Serialization
open System.IO

open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table

open MBrace.Core
open MBrace.Store
open MBrace.Store.Internals
open MBrace.Runtime.Vagabond

open MBrace.Azure.Store.TableEntities

/// Azure Table store CloudAtom implementation
[<AutoSerializable(true) ; Sealed; DataContract>]
type Atom<'T> internal (table, pk, rk, connectionString : string) =
    
    [<DataMember(Name = "ConnectionString")>]
    let connectionString = connectionString
    [<DataMember(Name = "Table")>]
    let table = table
    [<DataMember(Name = "PartitionKey")>]
    let pk = pk
    [<DataMember(Name = "RowKey")>]
    let rk = rk

    [<IgnoreDataMember>]
    let mutable client = Table.getClient(CloudStorageAccount.Parse connectionString)

    [<OnDeserialized>]
    let _onDeserialized (_ : StreamingContext) =
        client <- Table.getClient(CloudStorageAccount.Parse connectionString )

    interface ICloudAtom<'T> with

        member this.Id = sprintf "%s/%s/%s" table pk rk

        member this.Update(updater: 'T -> 'T, ?maxRetries : int): Local<unit> = 
            async {
                let interval = let r = new Random() in r.Next(2,10) 
                let maxInterval = 5000
                let maxRetries = defaultArg maxRetries Int32.MaxValue
                let rec update currInterval count = async {
                    if count >= maxRetries then
                        return raise <| exn("Maximum number of retries exceeded.")
                    else
                        let! e = Table.read<FatEntity> client table pk rk
                        let oldValue = VagabondRegistry.Instance.Serializer.UnPickle<'T> (e.GetPayload()) 
                        let newValue = updater oldValue
                        let newBinary = VagabondRegistry.Instance.Serializer.Pickle newValue
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
            } |> Cloud.OfAsync
             
        member this.Dispose(): Local<unit> = 
            async {
                let! e = Table.read<FatEntity> client table pk rk
                return! Table.delete<FatEntity> client table e
            } |> Cloud.OfAsync

        member this.Force(newValue: 'T): Local<unit> = 
            async {
                let! e = Table.read<FatEntity> client table pk rk
                let newBinary = VagabondRegistry.Instance.Serializer.Pickle newValue
                let e = new FatEntity(e.PartitionKey, String.Empty, newBinary, ETag = "*")
                let! _ = Table.merge client table e
                return ()
            } |> Cloud.OfAsync

        member this.Value : Local<'T> = 
            async {
                let! e = Table.read<FatEntity> client table pk rk
                let value = VagabondRegistry.Instance.Serializer.UnPickle<'T> (e.GetPayload())
                return value
            } |> Cloud.OfAsync

/// Store implementation that uses a Azure Blob Storage as backend.
[<Sealed; DataContract>]
type AtomProvider private (connectionString : string) =
    
    [<DataMember(Name = "ConnectionString")>]
    let connectionString = connectionString

    [<IgnoreDataMember>]
    let mutable client = 
        let acc = CloudStorageAccount.Parse(connectionString)
        Table.getClient acc

    [<OnDeserialized>]
    let _onDeserialized (_ : StreamingContext) =
        let acc = CloudStorageAccount.Parse(connectionString)
        client <- Table.getClient acc

    /// <summary>
    ///     Creates an Azure table store based atom provider that
    ///     connects to storage account with provided connection sting.
    /// </summary>
    /// <param name="connectionString">Azure storage account connection string.</param>
    static member Create (connectionString : string) =
        new AtomProvider(connectionString)

    interface ICloudAtomProvider with
        member this.Id = client.StorageUri.PrimaryUri.ToString()
            
        member this.Name = "Azure Table Storage Atom Provider" 

        member this.IsSupportedValue(value: 'T) : bool = 
            VagabondRegistry.Instance.Serializer.ComputeSize value <= int64 TableEntityConfig.MaxPayloadSize
        
        member this.CreateUniqueContainerName() = Table.getRandomName()

        member this.CreateAtom(path, initial: 'T) = 
                async {
                    let binary = VagabondRegistry.Instance.Serializer.Pickle initial
                    let e = new FatEntity(guid(), String.Empty, binary)
                    do! Table.insert<FatEntity> client path e
                    return new Atom<'T>(path, e.PartitionKey, e.RowKey, connectionString) :> ICloudAtom<'T>
                }

        member this.DisposeContainer(path) =
            async {
                do! client.GetTableReference(path).DeleteIfExistsAsync()
            }