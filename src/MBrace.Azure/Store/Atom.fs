namespace MBrace.Azure.Store

open System
open System.Runtime.Serialization
open System.IO

open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities

/// CloudAtom implementation on top of Azure table store.
[<AutoSerializable(true) ; Sealed; DataContract>]
type TableAtom<'T> internal (table : string, partitionKey : string, rowKey : string, account : AzureStorageAccount) =
    
    [<DataMember(Name = "StorageAccount")>]
    let account = account
    [<DataMember(Name = "Table")>]
    let table = table
    [<DataMember(Name = "PartitionKey")>]
    let partitionKey = partitionKey
    [<DataMember(Name = "RowKey")>]
    let rowKey = rowKey
    
    let getValueAsync() = async {
        let! e = Table.read<FatEntity> account table partitionKey rowKey
        let value = VagabondRegistry.Instance.Serializer.UnPickle<'T> (e.GetPayload())
        return value
    } 

    interface CloudAtom<'T> with

        member this.Id = sprintf "%s/%s/%s" table partitionKey rowKey
        
        member x.Transact(transaction: 'T -> 'R * 'T, maxRetries: int option): Async<'R> = async {
            let serializer = VagabondRegistry.Instance.Serializer
            let interval = let r = new Random() in r.Next(2,10)
            let maxInterval = 5000
            let maxRetries = defaultArg maxRetries Int32.MaxValue
            let rec update currInterval count = async {
                if count >= maxRetries then
                    return raise <| exn("Maximum number of retries exceeded.")
                else
                    let! e = Table.read<FatEntity> account table partitionKey rowKey
                    let oldValue = VagabondRegistry.Instance.Serializer.UnPickle<'T> (e.GetPayload()) 
                    let returnValue, newValue = transaction oldValue
                    let newBinary = serializer.Pickle newValue
                    let e = new FatEntity(e.PartitionKey, String.Empty, newBinary, ETag = e.ETag)
                    let! result = Async.Catch <| Table.merge account table e
                    match result with
                    | Choice1Of2 _ -> return returnValue
                    | Choice2Of2 e when StoreException.PreconditionFailed e -> 
                        do! Async.Sleep currInterval
                        return! update (min (interval * currInterval) maxInterval) (count+1)
                    | Choice2Of2 e -> return raise e
            }
            return! update interval 0
        } 

        member this.Dispose(): Async<unit> = async {
            let! e = Table.read<FatEntity> account table partitionKey rowKey
            return! Table.delete<FatEntity> account table e
        }

        member this.Force(newValue: 'T): Async<unit> = async {
            let! e = Table.read<FatEntity> account table partitionKey rowKey
            let newBinary = VagabondRegistry.Instance.Serializer.Pickle newValue
            let e = new FatEntity(e.PartitionKey, String.Empty, newBinary, ETag = "*")
            let! _ = Table.merge account table e
            return ()
        }

        member this.GetValueAsync () : Async<'T> = getValueAsync()
        member this.Value : 'T = getValueAsync () |> Async.RunSync

/// CloudAtom provider implementation on top of Azure table store.
[<Sealed; DataContract>]
type TableAtomProvider private (account : AzureStorageAccount, defaultTable : string) =
    
    [<DataMember(Name = "Account")>]
    let account = account

    [<DataMember(Name = "DefaultTable")>]
    let defaultTable = defaultTable

    /// <summary>
    ///     Creates an Azure table store based atom provider that
    ///     connects to provided storage account instance.
    /// </summary>
    /// <param name="connectionString">Azure storage account connection string.</param>
    static member Create (account : AzureStorageAccount, ?defaultTable : string) =
        ignore account.ConnectionString // force check that connection string is present in current host
        let defaultTable = match defaultTable with Some d -> Validate.tableName d ; d | None -> Table.getRandomName()
        new TableAtomProvider(account, defaultTable)

    /// <summary>
    ///     Creates an Azure table store based atom provider that
    ///     connects to storage account with provided connection sting.
    /// </summary>
    /// <param name="connectionString">Azure storage account connection string.</param>
    static member Create (connectionString : string, ?defaultTable : string) =
        TableAtomProvider.Create(AzureStorageAccount.Parse connectionString, ?defaultTable = defaultTable)

    interface ICloudAtomProvider with
        member this.Id = account.TableClient.StorageUri.PrimaryUri.ToString()
            
        member this.Name = "Azure Table Storage Atom Provider"
        
        member this.DefaultContainer = defaultTable
        member this.WithDefaultContainer (table : string) = 
            Validate.tableName table
            new TableAtomProvider(account, table) :> _

        member this.IsSupportedValue(value: 'T) : bool = 
            VagabondRegistry.Instance.Serializer.ComputeSize value <= int64 FatEntityConfiguration.MaxPayloadSize
        
        member this.CreateUniqueContainerName() = Table.getRandomName()

        member this.CreateAtom(table : string, initial: 'T) = async {
            let binary = VagabondRegistry.Instance.Serializer.Pickle initial
            let e = new FatEntity(guid(), String.Empty, binary)
            do! Table.insert<FatEntity> account table e
            return new TableAtom<'T>(table, e.PartitionKey, e.RowKey, account) :> CloudAtom<'T>
        }

        member this.DisposeContainer(table : string) = async {
            do! account.TableClient.GetTableReference(table).DeleteIfExistsAsync()
        }