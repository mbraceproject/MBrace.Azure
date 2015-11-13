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

[<AutoOpen>]
module private TableAtomConfig =
    
    [<Literal>]
    let defaultRowKey = ""

/// CloudAtom implementation on top of Azure table store.
[<AutoSerializable(true) ; Sealed; DataContract>]
type TableAtom<'T> internal (table : string, partitionKey : string, account : AzureStorageAccount) =
    
    [<DataMember(Name = "StorageAccount")>]
    let account = account
    [<DataMember(Name = "Table")>]
    let table = table
    [<DataMember(Name = "PartitionKey")>]
    let partitionKey = partitionKey
    
    let getValueAsync() = async {
        let! e = Table.read<FatEntity> account table partitionKey defaultRowKey
        let value = ProcessConfiguration.BinarySerializer.UnPickle<'T> (e.GetPayload())
        return value
    } 

    interface CloudAtom<'T> with
        member this.Container = table
        member this.Id = partitionKey
        
        member x.TransactAsync(transaction: 'T -> 'R * 'T, maxRetries: int option): Async<'R> = async {
            let serializer = ProcessConfiguration.BinarySerializer
            let interval = let r = new Random() in r.Next(2,10)
            let maxInterval = 5000
            let maxRetries = defaultArg maxRetries Int32.MaxValue
            let rec update currInterval count = async {
                if count >= maxRetries then
                    return raise <| exn("Maximum number of retries exceeded.")
                else
                    let! e = Table.read<FatEntity> account table partitionKey defaultRowKey
                    let oldValue = ProcessConfiguration.BinarySerializer.UnPickle<'T> (e.GetPayload()) 
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
            let! e = Table.read<FatEntity> account table partitionKey defaultRowKey
            return! Table.delete<FatEntity> account table e
        }

        member this.ForceAsync(newValue: 'T): Async<unit> = async {
            let! e = Table.read<FatEntity> account table partitionKey defaultRowKey
            let newBinary = ProcessConfiguration.BinarySerializer.Pickle newValue
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
        TableAtomProvider.Create(AzureStorageAccount.FromConnectionString connectionString, ?defaultTable = defaultTable)

    interface ICloudAtomProvider with
        member this.Id = account.TableClient.StorageUri.PrimaryUri.ToString()
            
        member this.Name = "Azure Table Storage CloudAtom Provider"
        
        member this.DefaultContainer = defaultTable
        member this.WithDefaultContainer (table : string) = 
            Validate.tableName table
            new TableAtomProvider(account, table) :> _

        member this.IsSupportedValue(value: 'T) : bool = 
            ProcessConfiguration.BinarySerializer.ComputeSize value <= int64 FatEntityConfiguration.MaxPayloadSize
        
        member this.GetRandomContainerName() = Table.getRandomNameWithPrefix "cloudAtom"
        member this.GetRandomAtomIdentifier() = sprintf "cloudAtom-%s" <| mkUUID()

        member this.CreateAtom<'T>(table : string, partitionKey : string, initial: 'T) = async {
            Validate.tableName table
            let binary = ProcessConfiguration.BinarySerializer.Pickle initial
            let e = new FatEntity(partitionKey, defaultRowKey, binary)
            do! Table.insert<FatEntity> account table e
            return new TableAtom<'T>(table, e.PartitionKey, account) :> CloudAtom<'T>
        }

        member this.GetAtomById<'T>(table : string, partitionKey : String) = async {
            let! e = Table.read<FatEntity> account table partitionKey defaultRowKey
            let p = e.GetPayload()
            let _ = ProcessConfiguration.BinarySerializer.UnPickle<'T>(p)
            return new TableAtom<'T>(table, partitionKey, account) :> CloudAtom<'T>
        }

        member this.DisposeContainer(table : string) = async {
            do! account.TableClient.GetTableReference(table).DeleteIfExistsAsync()
        }