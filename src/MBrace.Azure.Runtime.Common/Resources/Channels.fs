namespace Nessos.MBrace.Azure.Runtime.Resources

open System
open System.IO
open System.Runtime.Serialization
open Microsoft.ServiceBus.Messaging
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace
open Nessos.MBrace.Store
open Nessos.MBrace.Azure.Runtime.Common


// Implementation of Channels over ServiceBus Queues.
//
// TODO : Implement MBrace.Core interfaces

[<AutoSerializableAttribute(true)>]
type SendPort<'T> internal (queueName, config : ConfigurationId) =
    
    [<NonSerialized>]
    let queueClient = ConfigurationRegistry.Resolve<ClientProvider>(config).QueueClient(queueName)

    interface ISendPort<'T> with

        member __.Send(message : 'T) : Async<unit> = 
            async {
                let bin = Configuration.Serializer.Pickle(message)
                use ms = new MemoryStream(bin) in ms.Position <- 0L
                let msg = new BrokeredMessage(ms)
                do! queueClient.SendAsync(msg)
            }

[<AutoSerializableAttribute(true)>]
type ReceivePort<'T> internal (queueName, config : ConfigurationId) =
    
    [<NonSerialized>]
    let queueClient = ConfigurationRegistry.Resolve<ClientProvider>(config).QueueClient(queueName)

    interface IReceivePort<'T> with

        member __.Dispose () : Async<unit> =
            async {
                do! ConfigurationRegistry.Resolve<ClientProvider>(config).NamespaceClient.DeleteQueueAsync(queueName)
            }

        // TODO : Receive semantics

        member __.Receive(?timeout : int) : Async<'T> =
            async {
                let timeout = 
                    match timeout with 
                    | Some timeout -> TimeSpan.FromMilliseconds(float timeout)
                    | None -> TimeSpan.MaxValue

                let! (msg : BrokeredMessage) = queueClient.ReceiveAsync(timeout)

                if msg <> null then 
                    try
                        use stream = msg.GetBody<Stream>()
                        return Configuration.Serializer.Deserialize<'T>(stream)
                    finally
                        msg.Complete()
                else
                    return raise <| TimeoutException()
            }

[<AutoSerializableAttribute(false)>]
type ChannelProvider private (config : ConfigurationId) =
    
    interface ICloudChannelProvider with
        member __.Id = raise <| NotImplementedException("Id")

        member __.CreateChannel<'T> () =
            async {
                let queueName = guid()
                let ns = ConfigurationRegistry.Resolve<ClientProvider>(config).NamespaceClient
                let qd = new QueueDescription(queueName)
                qd.EnablePartitioning <- true
                qd.DefaultMessageTimeToLive <- MaxTTL
                qd.LockDuration <- MaxLockDuration
                do! ns.CreateQueueAsync(qd)
                return new SendPort<'T>(queueName, config) :> ISendPort<'T>, 
                        new ReceivePort<'T>(queueName, config) :> IReceivePort<'T>
            }

    static member Create(config : ConfigurationId) =
        new ChannelProvider(config)
