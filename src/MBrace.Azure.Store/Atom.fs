namespace MBrace.Azure.Store

open System
open System.Runtime.Serialization
open System.IO

open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

open MBrace.Azure.Store.TableEntities

/// Azure Table store CloudAtom implementation
[<AutoSerializable(true) ; Sealed; DataContract>]
type Atom<'T> internal (table : string, partitionKey : string, rowKey : string, connectionString : string) =
    
    [<DataMember(Name = "ConnectionString")>]
    let connectionString = connectionString
    [<DataMember(Name = "Table")>]
    let table = table
    [<DataMember(Name = "PartitionKey")>]
    let partitionKey = partitionKey
    [<DataMember(Name = "RowKey")>]
    let rowKey = rowKey

    [<IgnoreDataMember>]
    let mutable client = Table.getClient(CloudStorageAccount.Parse connectionString)

    [<OnDeserialized>]
    let _onDeserialized (_ : StreamingContext) =
        client <- Table.getClient(CloudStorageAccount.Parse connectionString)

    
    let getValueAsync() = async {
        let! e = Table.read<FatEntity> client table partitionKey rowKey
        let value = VagabondRegistry.Instance.Serializer.UnPickle<'T> (e.GetPayload())
        return value
    } 

    interface CloudAtom<'T> with

        member this.Id = sprintf "%s/%s/%s" table partitionKey rowKey
        
        member x.Transact(transaction: 'T -> 'R * 'T, maxRetries: int option): Async<'R> = 
            async {
                let interval = let r = new Random() in r.Next(2,10) 
                let maxInterval = 5000
                let maxRetries = defaultArg maxRetries Int32.MaxValue
                let rec update currInterval count = async {
                    if count >= maxRetries then
                        return raise <| exn("Maximum number of retries exceeded.")
                    else
                        let! e = Table.read<FatEntity> client table partitionKey rowKey
                        let oldValue = VagabondRegistry.Instance.Serializer.UnPickle<'T> (e.GetPayload()) 
                        let returnValue, newValue = transaction oldValue
                        let newBinary = VagabondRegistry.Instance.Serializer.Pickle newValue
                        let e = new FatEntity(e.PartitionKey, String.Empty, newBinary, ETag = e.ETag)
                        let! result = Async.Catch <| Table.merge client table e
                        match result with
                        | Choice1Of2 _ -> return returnValue
                        | Choice2Of2 e when Table.PreconditionFailed e -> 
                            do! Async.Sleep currInterval
                            return! update (min (interval * currInterval) maxInterval) (count+1)
                        | Choice2Of2 e -> return raise e
                }
                return! update interval 0
            } 

        member this.Dispose(): Async<unit> = 
            async {
                let! e = Table.read<FatEntity> client table partitionKey rowKey
                return! Table.delete<FatEntity> client table e
            }

        member this.Force(newValue: 'T): Async<unit> = 
            async {
                let! e = Table.read<FatEntity> client table partitionKey rowKey
                let newBinary = VagabondRegistry.Instance.Serializer.Pickle newValue
                let e = new FatEntity(e.PartitionKey, String.Empty, newBinary, ETag = "*")
                let! _ = Table.merge client table e
                return ()
            }

        member this.GetValueAsync () : Async<'T> = getValueAsync()
        member this.Value : 'T = getValueAsync () |> Async.RunSync

/// Store implementation that uses a Azure Blob Storage as backend.
[<Sealed; DataContract>]
type AtomProvider private (connectionString : string, defaultTable : string) =
    // TODO: validate table inputs
    
    [<DataMember(Name = "ConnectionString")>]
    let connectionString = connectionString

    [<DataMember(Name = "DefaultTable")>]
    let defaultTable = defaultTable

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
    static member Create (connectionString : string, ?defaultTable : string) =
        let defaultTable = match defaultTable with Some d -> d | None -> Table.getRandomName()
        new AtomProvider(connectionString, defaultTable)

    interface ICloudAtomProvider with
        member this.Id = client.StorageUri.PrimaryUri.ToString()
            
        member this.Name = "Azure Table Storage Atom Provider"
        
        member this.DefaultContainer = defaultTable
        member this.WithDefaultContainer table = new AtomProvider(connectionString, table) :> _

        member this.IsSupportedValue(value: 'T) : bool = 
            VagabondRegistry.Instance.Serializer.ComputeSize value <= int64 TableEntityConfig.MaxPayloadSize
        
        member this.CreateUniqueContainerName() = Table.getRandomName()

        member this.CreateAtom(path, initial: 'T) = 
                async {
                    let binary = VagabondRegistry.Instance.Serializer.Pickle initial
                    let e = new FatEntity(guid(), String.Empty, binary)
                    do! Table.insert<FatEntity> client path e
                    return new Atom<'T>(path, e.PartitionKey, e.RowKey, connectionString) :> CloudAtom<'T>
                }

        member this.DisposeContainer(path : string) =
            async {
                do! client.GetTableReference(path).DeleteIfExistsAsync()
            }