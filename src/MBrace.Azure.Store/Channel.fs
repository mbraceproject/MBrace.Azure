namespace MBrace.Azure.Store

open Microsoft.ServiceBus.Messaging
open MBrace
open MBrace.Continuation
open MBrace.Store
open System
open System.IO
open System.Runtime.Serialization
open Microsoft.ServiceBus

// TODO : Channel semantics.

[<AutoSerializable(true) ; Sealed; DataContract>]
type SendPort<'T> internal (queuePath, connectionString, serializer : ISerializer) =
    
    [<DataMember(Name = "ConnectionString")>]
    let connectionString = connectionString
    [<DataMember(Name = "Serializer")>]
    let serializer = serializer
    [<DataMember(Name = "QueuePath")>]
    let queuePath = queuePath

    [<IgnoreDataMember>]
    let mutable client = QueueClient.CreateFromConnectionString(connectionString, queuePath, ReceiveMode.ReceiveAndDelete)

    [<OnDeserialized>]
    let _onDeserialized (_ : StreamingContext) =
        client <- QueueClient.CreateFromConnectionString(connectionString, queuePath, ReceiveMode.ReceiveAndDelete)

    interface ISendPort<'T> with
        member x.Id : string = queuePath
        
        member __.Send(message : 'T) : Cloud<unit> = 
            async {
                let bin = pickle message serializer
                use ms = new MemoryStream(bin) in ms.Position <- 0L
                let msg = new BrokeredMessage(ms)
                do! client.SendAsync(msg)
            } |> Cloud.OfAsync

[<AutoSerializable(true) ; Sealed; DataContract>]
type ReceivePort<'T> internal (queuePath, connectionString, serializer : ISerializer) =
    [<DataMember(Name = "ConnectionString")>]
    let connectionString = connectionString
    [<DataMember(Name = "Serializer")>]
    let serializer = serializer
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

        member __.Dispose () : Cloud<unit> = 
            nsClient.DeleteQueueAsync(queuePath)
            |> Async.AwaitTask
            |> Cloud.OfAsync

        member __.Receive(?timeout : int) : Cloud<'T> =
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
                return serializer.Deserialize<'T>(stream, false)
            } |> Cloud.OfAsync

[<Sealed; DataContract>]
type ChannelProvider private (connectionString : string, serializer : ISerializer) =
    
    [<DataMember(Name = "ConnectionString")>]
    let connectionString = connectionString
    [<DataMember(Name = "Serializer")>]
    let serializer = serializer

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
                return new SendPort<'T>(queuePath, connectionString, serializer) :> ISendPort<'T>, 
                        new ReceivePort<'T>(queuePath, connectionString, serializer) :> IReceivePort<'T>
            }

    static member Create(connectionString : string, serializer : ISerializer) : ICloudChannelProvider =
        new ChannelProvider(connectionString, serializer) :> _
