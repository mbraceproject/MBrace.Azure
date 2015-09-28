namespace MBrace.Azure.Store

open System
open System.IO
open System.Runtime.Serialization

open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities

[<AutoSerializable(true) ; Sealed; DataContract>]
type Queue<'T> internal (queuePath, account : AzureServiceBusAccount) =
    
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
        member x.Count: Async<int64> = async {
            let! (qd : QueueDescription) = account.NamespaceManager.GetQueueAsync(queuePath)
            return qd.MessageCount
        }
        
        member x.Enqueue(message: 'T): Async<unit> = async {
            let bin = VagabondRegistry.Instance.Serializer.Pickle message
            use ms = new MemoryStream(bin) in ms.Position <- 0L
            let msg = new BrokeredMessage(ms)
            do! client.SendAsync(msg)
        }
        
        member x.EnqueueBatch(messages: seq<'T>): Async<unit> = async {
            return!
                messages
                |> Seq.map (fun item ->
                    let bin = VagabondRegistry.Instance.Serializer.Pickle item
                    use ms = new MemoryStream(bin) in ms.Position <- 0L
                    new BrokeredMessage(ms))
                |> client.SendBatchAsync
        }
        
        member x.TryDequeue(): Async<'T option> = async {
            let! (msg : BrokeredMessage) = client.ReceiveAsync()
            match msg with
            | null -> return None
            | msg ->
                use stream = msg.GetBody<Stream>()
                return Some(VagabondRegistry.Instance.Serializer.Deserialize<'T>(stream))
        }
        
        member x.Dequeue(?timeout: int): Async<'T> = async {
            let! msg =
                match timeout with 
                | Some timeout -> 
                    async {
                        let timeout = TimeSpan.FromMilliseconds(float timeout)
                        let! msg = client.ReceiveAsync(timeout)
                        if msg <> null then return msg
                        else return! Async.Raise(TimeoutException())
                    }
                | None -> 
                    let rec aux _ = async {
                        let! msg = client.ReceiveAsync()
                        if msg <> null then return msg
                        else return! aux ()
                    }
                    aux ()

            use stream = msg.GetBody<Stream>()
            return VagabondRegistry.Instance.Serializer.Deserialize<'T>(stream)
        }

        member x.Dispose(): Async<unit> = async {
            return!
                account.NamespaceManager.DeleteQueueAsync(queuePath)
                |> Async.AwaitTaskCorrect
        }


/// MBrace Channel provider over Azure Service Bus queues
[<Sealed; DataContract>]
type QueueProvider private (account : AzureServiceBusAccount) =
    
    [<DataMember(Name = "Account")>]
    let account = account

    interface ICloudQueueProvider with
        member x.CreateUniqueContainerName() : string = ""
        member x.DefaultContainer = ""
        member x.WithDefaultContainer _ = x :> _
        member x.DisposeContainer(queuePath : string) : Async<unit> = async { do! account.NamespaceManager.DeleteQueueAsync(queuePath) }

        member __.Name = "Service Bus Channel Provider"
        
        member __.Id = account.AccountName

        member __.CreateQueue<'T> (_ : string) =
            async {
                let queuePath = sprintf "queue_%s" <| guid()
                let qd = new QueueDescription(queuePath)
                qd.SupportOrdering <- true
                qd.DefaultMessageTimeToLive <- TimeSpan.MaxValue
                do! account.NamespaceManager.CreateQueueAsync(qd)
                return new Queue<'T>(queuePath, account) :> CloudQueue<'T>
            }

    /// <summary>
    ///     Creates an Azure Service bus queue connection string.
    /// </summary>
    /// <param name="connectionString">Azure service bus connection string.</param>
    static member Create(connectionString : string) =
        let account = AzureServiceBusAccount.Parse connectionString
        new QueueProvider(account)
