namespace Nessos.MBrace.Azure.Runtime.Resources

open System
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Common

[<AutoSerializableAttribute(true)>]
type Atom<'T> internal (table, pk, rk, config) =

    [<NonSerializedAttribute>]
    let pickler = Configuration.Serializer

    member this.Delete(id: string): Async<unit> = 
        async {
            let! e = Table.read<FatEntity> config table pk rk
            return! Table.delete<FatEntity> config table e
        }
        
    member this.Update(updater: 'T -> 'T, ?maxRetries : int): Async<unit> = 
        async {
            let interval = let r = new Random() in r.Next(2,10) 
            let maxInterval = 5000
            let maxRetries = defaultArg maxRetries Int32.MaxValue
            let rec update currInterval count = async {
                if count >= maxRetries then
                    return raise <| exn("Maximum number of retries exceeded.")
                else
                    let! e = Table.read<FatEntity> config table pk rk
                    let oldValue = pickler.UnPickle<'T>(e.GetPayload())
                    let newValue = updater oldValue
                    let newBinary = pickler.Pickle<'T>(newValue)
                    let e = new FatEntity(e.PartitionKey, String.Empty, newBinary, ETag = e.ETag)
                    let! result = Async.Catch <| Table.merge config table e
                    match result with
                    | Choice1Of2 _ -> return ()
                    | Choice2Of2 e when Table.PreconditionFailed e -> 
                        do! Async.Sleep currInterval
                        return! update (min (interval * currInterval) maxInterval) (count+1)
                    | Choice2Of2 e -> return raise e
            }

            return! update interval 0
        }       

    member this.Force(newValue: 'T): Async<unit> = 
        async {
            let! e = Table.read<FatEntity> config table pk rk
            let newBinary = pickler.Pickle<'T>(newValue)
            let e = new FatEntity(e.PartitionKey, String.Empty, newBinary, ETag = "*")
            let! _ = Table.merge config table e
            return ()
        }

    member this.GetValue(): Async<'T> = 
        async {
            let! e = Table.read<FatEntity> config table pk rk
            let value = pickler.UnPickle<'T>(e.GetPayload())
            return value
        }

[<AutoSerializableAttribute(false)>]
type AtomProvider private (table, config : ConfigurationId) =
        
    member this.IsSupportedValue(value: 'T) : bool = 
        Configuration.Serializer.ComputeSize(value) <= TableEntityUtils.MaxPayloadSize
        
    member this.Create(initial: 'T): Async<Atom<'T>> = 
            async {
                let binary = Configuration.Serializer.Pickle(initial)
                let e = new FatEntity(guid(), String.Empty, binary)
                do! Table.insert<FatEntity> config table e
                return new Atom<'T>(table, e.PartitionKey, e.RowKey, config)
            }

    static member Create(table, config : ConfigurationId) =
        new AtomProvider(table, config)
