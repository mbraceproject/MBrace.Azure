namespace MBrace.Azure.Store

open System
open System.IO
open System.Runtime.Serialization
open System.Collections
open System.Collections.Generic

open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities

/// MBrace CloudDictionary implementation on top of Azure Table Store.
[<AutoSerializable(true) ; Sealed; DataContract>]
type TableDictionary<'T> internal (tableName : string, account : AzureStorageAccount) = 
    
    [<DataMember(Name = "Account")>]
    let account = account
    [<DataMember(Name = "Table")>]
    let tableName = tableName

    let getEntitiesAsync() = Table.readAll<FatEntity> account tableName

    let getSeqAsync() = async {
        let serializer = ProcessConfiguration.BinarySerializer
        let! entities = getEntitiesAsync()
        return entities |> Seq.map (fun entity -> new KeyValuePair<_,_>(entity.PartitionKey, serializer.UnPickle<'T>(entity.GetPayload())))
    }

    let getSeq() = getSeqAsync() |> Async.RunSync

    interface seq<KeyValuePair<string,'T>> with
        member __.GetEnumerator() = getSeq().GetEnumerator() :> IEnumerator
        member __.GetEnumerator() = getSeq().GetEnumerator()
        
    interface CloudDictionary<'T> with
        member x.IsKnownCount: bool = false
        member x.IsMaterialized : bool = false
        member x.IsKnownSize: bool = false
        
        member this.Add(key: string, value : 'T): Async<unit> = async {
            let binary = ProcessConfiguration.BinarySerializer.Pickle value
            let e = new FatEntity(key, String.Empty, binary)
            do! Table.insert<FatEntity> account tableName e
        }
        
        member this.Transact(key: string, transacter: 'T option -> 'R * 'T, maxRetries: int option): Async<'R> = async {
            let serializer = ProcessConfiguration.BinarySerializer
            let rec transact (e : FatEntity) (count : int) : Async<'R> = async { 
                match maxRetries with
                | Some maxRetries when count >= maxRetries -> 
                    return raise <| exn("Maximum number of retries exceeded.")
                | _ -> ()

                let value = 
                    match e with
                    | null -> None
                    | e -> Some(serializer.UnPickle<'T>(e.GetPayload()))
                        
                let returnValue, newValue = transacter value
                let binary = serializer.Pickle newValue
                let e' = new FatEntity(key, String.Empty, binary)
                match value with
                | None ->
                    let! result = Async.Catch <| Table.insert<FatEntity> account tableName e'
                    match result with
                    | Choice1Of2 () -> return returnValue
                    | Choice2Of2 ex when StoreException.Conflict ex ->
                        let! e = Table.read<FatEntity> account tableName key String.Empty
                        return! transact e (count + 1)
                    | Choice2Of2 ex -> return raise ex
                | Some _ ->
                    e'.ETag <- e.ETag
                    let! result = Async.Catch <| Table.merge<FatEntity> account tableName e'
                    match result with
                    | Choice1Of2 _ -> return returnValue
                    | Choice2Of2 ex when StoreException.PreconditionFailed ex -> 
                        let! e = Table.read<FatEntity> account tableName key String.Empty
                        return! transact e (count + 1)
                    | Choice2Of2 ex -> return raise ex
            }
            let! e = Table.read<FatEntity> account tableName key String.Empty
            return! transact e 0
        }

        member this.ContainsKey(key: string): Async<bool> = 
            async {
                let! e = Table.read<FatEntity> account tableName key String.Empty
                return e <> null
            }
        
        member this.GetCount () : Async<int64> = async { let! entities = getEntitiesAsync() in return int64 entities.Count }
        member this.GetSize () : Async<int64> = async { let! entities = getEntitiesAsync() in return int64 entities.Count }

        member this.Dispose(): Async<unit> = async {
            let! _ = account.TableClient.GetTableReference(tableName).DeleteAsync() |> Async.AwaitTaskCorrect
            return ()
        }
        
        member this.Id: string = tableName
        
        member this.Remove(key: string): Async<bool> = async {
            try
                do! Table.delete account tableName (TableEntity(key, String.Empty, ETag = "*"))
                return true
            with ex -> 
                if StoreException.NotFound ex then return false else return raise ex
        }
        
        member this.ToEnumerable(): Async<seq<Collections.Generic.KeyValuePair<string,'T>>> = async {
            return! getSeqAsync()
        }
        
        member this.TryAdd(key: string, value: 'T): Async<bool> = async {
            try
                do! (this :> CloudDictionary<'T>).Add(key, value)
                return true
            with ex ->
                if StoreException.Conflict ex then return false else return raise ex
        }
        
        member x.TryFind(key: string): Async<'T option> = async {
            let! e = Table.read<FatEntity> account tableName key String.Empty
            match e with
            | null -> return None
            | e ->
                let value = ProcessConfiguration.BinarySerializer.UnPickle<'T>(e.GetPayload())
                return Some value
        }

/// CloudDictionary provider implementation on top of Azure table store.
[<Sealed; DataContract>]
type TableDictionaryProvider private (account : AzureStorageAccount) =
    
    [<DataMember(Name = "Account")>]
    let account = account

    /// <summary>
    ///     Creates a TableDirectionaryProvider instance using provided Azure storage account.
    /// </summary>
    /// <param name="account">Azure storage account.</param>
    static member Create(account : AzureStorageAccount) =
        ignore account.ConnectionString // ensure that connection string is present in the current context
        new TableDictionaryProvider(account)

    /// <summary>
    ///     Creates a TableDirectionaryProvider instance using provided Azure storage connection string.
    /// </summary>
    /// <param name="connectionString">Azure storage connection string.</param>
    static member Create(connectionString : string) =
        TableDictionaryProvider.Create(AzureStorageAccount.Parse connectionString)

    interface ICloudDictionaryProvider with
        member x.Id = account.TableClient.BaseUri.AbsolutePath
        member x.Name = "Azure TableStore CloudDictionary Provider"
        member x.GetRandomDictionaryId() = Table.getRandomNameWithPrefix "cloudDictionary"
        member x.CreateDictionary<'T>(tableName : string): Async<CloudDictionary<'T>> = async {
            Validate.tableName tableName
            let tableRef = account.TableClient.GetTableReference(tableName)
            do! tableRef.CreateIfNotExistsAsyncSafe(maxRetries = 3)
            return new TableDictionary<'T>(tableName, account) :> CloudDictionary<'T>
        }

        member x.GetById<'T>(tableName : string) : Async<CloudDictionary<'T>> = async {
            Validate.tableName tableName
            let tableRef = account.TableClient.GetTableReference(tableName)
            let! exists = tableRef.ExistsAsync()
            if exists then return new TableDictionary<'T>(tableName, account) :> CloudDictionary<'T>
            else
                return invalidOp <| sprintf "Could not locate CloudDictionary table '%s' in storage account '%s'" tableName account.AccountName
        }

        member x.IsSupportedValue(value: 'T): bool = 
            ProcessConfiguration.BinarySerializer.ComputeSize value <= int64 FatEntityConfiguration.MaxPayloadSize