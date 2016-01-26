namespace MBrace.Azure.Store

open System
open System.IO
open System.Runtime.Serialization

open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils.PrettyPrinters

open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities

/// CloudQueue implementation on top of Azure ServiceBus
[<AutoSerializable(true) ; Sealed; DataContract>]
type ServiceBusQueue<'T> internal (queuePath, account : AzureServiceBusAccount) =
    
    [<DataMember(Name = "Account")>]
    let account = account
    [<DataMember(Name = "QueuePath")>]
    let queuePath = queuePath

    [<IgnoreDataMember>]
    let mutable client = QueueClient.CreateFromConnectionString(account.ConnectionString, queuePath, ReceiveMode.ReceiveAndDelete)

    [<OnDeserialized>]
    let _onDeserialized (_ : StreamingContext) =
        client <- QueueClient.CreateFromConnectionString(account.ConnectionString, queuePath, ReceiveMode.ReceiveAndDelete)

    interface CloudQueue<'T> with
        member x.Id: string = queuePath
        member x.GetCountAsync (): Async<int64> = async {
            let! (qd : QueueDescription) = account.NamespaceManager.GetQueueAsync(queuePath) |> Async.AwaitTaskCorrect
            return qd.MessageCount
        }
        
        member x.EnqueueAsync(message: 'T): Async<unit> = async {
            let bin = ProcessConfiguration.BinarySerializer.Pickle message
            use ms = new MemoryStream(bin) 
            do ms.Position <- 0L
            let msg = new BrokeredMessage(ms)
            do! client.SendAsync(msg) |> Async.AwaitTaskCorrect
        }
        
        member x.EnqueueBatchAsync(messages: seq<'T>): Async<unit> = async {
            let serializer = ProcessConfiguration.BinarySerializer
            let createMsg (item : 'T) =
                let bin = serializer.Pickle item
                let ms = new MemoryStream(bin) in ms.Position <- 0L
                new BrokeredMessage(ms)

            return! messages |> Seq.map createMsg |> client.SendBatchAsync |> Async.AwaitTaskCorrect
        }
        
        member x.TryDequeueAsync(): Async<'T option> = async {
            let! (msg : BrokeredMessage) = client.ReceiveAsync() |> Async.AwaitTaskCorrect
            match msg with
            | null -> return None
            | msg ->
                use stream = msg.GetBody<Stream>()
                return Some(ProcessConfiguration.BinarySerializer.Deserialize<'T>(stream))
        }

        member x.DequeueBatchAsync(maxItems : int) : Async<'T []> = async {
            let serializer = ProcessConfiguration.BinarySerializer
            let readBody (msg : BrokeredMessage) = use stream = msg.GetBody<Stream>() in serializer.Deserialize<'T>(stream)
            let! messages = client.ReceiveBatchAsync(maxItems) |> Async.AwaitTaskCorrect
            return messages |> Seq.map readBody |> Seq.toArray
        }
        
        member x.DequeueAsync(?timeout: int): Async<'T> = async {
            let! msg = async {
                match timeout with 
                | Some timeout -> 
                    let timeout = TimeSpan.FromMilliseconds(float timeout)
                    let! msg = client.ReceiveAsync(timeout) |> Async.AwaitTaskCorrect
                    if msg <> null then return msg
                    else return! Async.Raise(TimeoutException())

                | None -> 
                    let rec aux _ = async {
                        let! msg = client.ReceiveAsync() |> Async.AwaitTaskCorrect
                        if msg <> null then return msg
                        else return! aux ()
                    }
                    return! aux ()
            }

            use stream = msg.GetBody<Stream>()
            return ProcessConfiguration.BinarySerializer.Deserialize<'T>(stream)
        }

        member x.Dispose(): Async<unit> = async {
            return!
                account.NamespaceManager.DeleteQueueAsync(queuePath)
                |> Async.AwaitTaskCorrect
        }


/// MBrace CloudQueue provider implemented on top of Azure Service Bus queues
[<Sealed; DataContract>]
type ServiceBusQueueProvider private (account : AzureServiceBusAccount) =
    
    [<DataMember(Name = "Account")>]
    let account = account

    interface ICloudQueueProvider with
        member __.Id = account.AccountName
        member __.Name = "Azure ServiceBus CloudQueue Provider"
        member __.GetRandomQueueName() = sprintf "cloudQueue-%O" <| mkUUID()

        member __.CreateQueue<'T> (queueId : string) = async {
            Validate.queueName queueId
            let qd = new QueueDescription(queueId)
            qd.SupportOrdering <- true
            qd.DefaultMessageTimeToLive <- TimeSpan.MaxValue
            qd.UserMetadata <- Type.prettyPrint<'T>
            let! result = account.NamespaceManager.CreateQueueAsync qd |> Async.AwaitTaskCorrect
            return new ServiceBusQueue<'T>(queueId, account) :> CloudQueue<'T>
        }

        member __.GetQueueById<'T> (queueId : string) : Async<CloudQueue<'T>> = async {
            Validate.queueName queueId
            let! (qd : QueueDescription) = account.NamespaceManager.GetQueueAsync queueId |> Async.AwaitTaskCorrect
            if qd.UserMetadata = Type.prettyPrint<'T> then
                return new ServiceBusQueue<'T>(queueId, account) :> CloudQueue<'T>
            else
                return invalidOp <| sprintf "Expected queue type '%s' but was '%s'" Type.prettyPrint<'T> qd.UserMetadata
        }

    /// <summary>
    ///     Creates an Azure Service bus Queue provider using provided azure service bus account.
    /// </summary>
    /// <param name="account">Azure service bus account.</param>
    static member Create(account : AzureServiceBusAccount) =
        ignore account.ConnectionString // check that connection string is present in current host context.
        new ServiceBusQueueProvider(account)

    /// <summary>
    ///     Creates an Azure Service bus Queue provider using provided azure service bus connection string.
    /// </summary>
    /// <param name="account">Azure service bus account.</param>
    static member Create(connectionString : string) =
        ServiceBusQueueProvider.Create(AzureServiceBusAccount.FromConnectionString connectionString)