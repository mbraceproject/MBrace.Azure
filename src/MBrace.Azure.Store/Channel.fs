namespace MBrace.Azure.Store

open System
open System.IO
open System.Runtime.Serialization

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals
open MBrace.Runtime.Vagabond

open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging

// TODO : Channel semantics.

[<AutoSerializable(true) ; Sealed; DataContract>]
type SendPort<'T> internal (queuePath, connectionString) =
    
    [<DataMember(Name = "ConnectionString")>]
    let connectionString = connectionString
    [<DataMember(Name = "QueuePath")>]
    let queuePath = queuePath

    [<IgnoreDataMember>]
    let mutable client = QueueClient.CreateFromConnectionString(connectionString, queuePath, ReceiveMode.ReceiveAndDelete)

    [<OnDeserialized>]
    let _onDeserialized (_ : StreamingContext) =
        client <- QueueClient.CreateFromConnectionString(connectionString, queuePath, ReceiveMode.ReceiveAndDelete)

    interface ISendPort<'T> with
        member x.Id : string = queuePath
        
        member __.Send(message : 'T) : Local<unit> = 
            async {
                let bin = VagabondRegistry.Instance.Serializer.Pickle message
                use ms = new MemoryStream(bin) in ms.Position <- 0L
                let msg = new BrokeredMessage(ms)
                do! client.SendAsync(msg)
            } |> Cloud.OfAsync

[<AutoSerializable(true) ; Sealed; DataContract>]
type ReceivePort<'T> internal (queuePath, connectionString) =
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

    interface IReceivePort<'T> with
        member __.Id : string = queuePath

        member __.Dispose () : Local<unit> = 
            nsClient.DeleteQueueAsync(queuePath)
            |> Async.AwaitTask
            |> Cloud.OfAsync

        member __.Receive(?timeout : int) : Local<'T> =
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
            } |> Cloud.OfAsync


/// MBrace Channel provider over Azure Service Bus queues
[<Sealed; DataContract>]
type ChannelProvider private (connectionString : string) =
    
    [<DataMember(Name = "ConnectionString")>]
    let connectionString = connectionString

    [<IgnoreDataMember>]
    let mutable nsClient = NamespaceManager.CreateFromConnectionString(connectionString)

    [<OnDeserialized>]
    let _onDeserialized (_ : StreamingContext) =
        nsClient <- NamespaceManager.CreateFromConnectionString(connectionString)

    interface ICloudChannelProvider with
        member x.CreateUniqueContainerName() : string = guid()
        
        member x.DisposeContainer(queuePath : string) : Async<unit> = async { do! nsClient.DeleteQueueAsync(queuePath) }

        member __.Name = "Service Bus Channel Provider"
        
        member __.Id = nsClient.Address.ToString()

        member __.CreateChannel<'T> (_ : string) =
            async {
                let queuePath = sprintf "channel_%s" <| guid()
                let qd = new QueueDescription(queuePath)
                qd.SupportOrdering <- true
                qd.DefaultMessageTimeToLive <- TimeSpan.MaxValue
                do! nsClient.CreateQueueAsync(qd)
                return new SendPort<'T>(queuePath, connectionString) :> ISendPort<'T>, 
                        new ReceivePort<'T>(queuePath, connectionString) :> IReceivePort<'T>
            }

    /// <summary>
    ///     Creates an Azure Service bus queue connection string.
    /// </summary>
    /// <param name="connectionString">Azure service bus connection string.</param>
    static member Create(connectionString : string) =
        new ChannelProvider(connectionString)
