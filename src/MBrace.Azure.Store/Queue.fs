namespace MBrace.Azure.Store

open System
open System.IO
open System.Runtime.Serialization

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging

[<AutoSerializable(true) ; Sealed; DataContract>]
type Queue<'T> internal (queuePath, connectionString) =
    
    [<DataMember(Name = "ConnectionString")>]
    let connectionString = connectionString
    [<DataMember(Name = "QueuePath")>]
    let queuePath = queuePath

    [<IgnoreDataMember>]
    let mutable client = QueueClient.CreateFromConnectionString(connectionString, queuePath, ReceiveMode.ReceiveAndDelete)

    [<IgnoreDataMember>]
    let mutable nsClient = NamespaceManager.CreateFromConnectionString(connectionString)

    [<OnDeserialized>]
    let _onDeserialized (_ : StreamingContext) =
        client <- QueueClient.CreateFromConnectionString(connectionString, queuePath, ReceiveMode.ReceiveAndDelete)
        nsClient <- NamespaceManager.CreateFromConnectionString(connectionString)

    interface CloudQueue<'T> with
        member x.Id: string = queuePath
        member x.Count: Async<int64> =
            async {
                let! queueDescription = nsClient.GetQueueAsync(queuePath)
                                        |> Async.AwaitTask
                return queueDescription.MessageCount
            }
        
        member x.Enqueue(message: 'T): Async<unit> = 
            async {
                let bin = VagabondRegistry.Instance.Serializer.Pickle message
                use ms = new MemoryStream(bin) in ms.Position <- 0L
                let msg = new BrokeredMessage(ms)
                do! client.SendAsync(msg)
            }
        
        member x.EnqueueBatch(messages: seq<'T>): Async<unit> = 
            async {
                return!
                    messages
                    |> Seq.map (fun item ->
                        let bin = VagabondRegistry.Instance.Serializer.Pickle item
                        use ms = new MemoryStream(bin) in ms.Position <- 0L
                        new BrokeredMessage(ms))
                    |> client.SendBatchAsync
            }
        
        member x.TryDequeue(): Async<'T option> = 
            async {
                let! (msg : BrokeredMessage) = client.ReceiveAsync()
                match msg with
                | null -> return None
                | msg ->
                    use stream = msg.GetBody<Stream>()
                    return Some(VagabondRegistry.Instance.Serializer.Deserialize<'T>(stream))
            }
        
        member x.Dequeue(?timeout: int): Async<'T> = 
            async {
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

        member x.Dispose(): Async<unit> = 
            nsClient.DeleteQueueAsync(queuePath)
            |> Async.AwaitTask


/// MBrace Channel provider over Azure Service Bus queues
[<Sealed; DataContract>]
type QueueProvider private (connectionString : string) =
    
    [<DataMember(Name = "ConnectionString")>]
    let connectionString = connectionString

    [<IgnoreDataMember>]
    let mutable nsClient = NamespaceManager.CreateFromConnectionString(connectionString)

    [<OnDeserialized>]
    let _onDeserialized (_ : StreamingContext) =
        nsClient <- NamespaceManager.CreateFromConnectionString(connectionString)

    interface ICloudQueueProvider with
        member x.CreateUniqueContainerName() : string = ""
        member x.DefaultContainer = ""
        member x.WithDefaultContainer _ = x :> _
        member x.DisposeContainer(queuePath : string) : Async<unit> = async { do! nsClient.DeleteQueueAsync(queuePath) }

        member __.Name = "Service Bus Channel Provider"
        
        member __.Id = nsClient.Address.ToString()

        member __.CreateQueue<'T> (_ : string) =
            async {
                let queuePath = sprintf "queue_%s" <| guid()
                let qd = new QueueDescription(queuePath)
                qd.SupportOrdering <- true
                qd.DefaultMessageTimeToLive <- TimeSpan.MaxValue
                do! nsClient.CreateQueueAsync(qd)
                return new Queue<'T>(queuePath, connectionString) :> CloudQueue<'T>
            }

    /// <summary>
    ///     Creates an Azure Service bus queue connection string.
    /// </summary>
    /// <param name="connectionString">Azure service bus connection string.</param>
    static member Create(connectionString : string) =
        new QueueProvider(connectionString)
