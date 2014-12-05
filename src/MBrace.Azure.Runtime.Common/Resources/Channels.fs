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
type SendPort<'T> internal (queueName, config : ConfigurationId) =
    
    let queueClient = ConfigurationRegistry.Resolve<ClientProvider>(config).QueueClient(queueName)

    interface ISendPort<'T> with

        member __.Send(message : 'T) : Async<unit> = 
            async {
                let bin = Configuration.Serializer.Pickle(message)
                use ms = new MemoryStream(bin) in ms.Position <- 0L
                let msg = new BrokeredMessage(ms)
                do! queueClient.SendAsync(msg)
            }

    interface ISerializable with
        member x.GetObjectData(info : SerializationInfo, context : StreamingContext) : unit = 
            info.AddValue("queueName", queueName, typeof<string>)
            info.AddValue("config", config, typeof<ConfigurationId>)
    
    new(info : SerializationInfo, context : StreamingContext) = 
        let queueName = info.GetValue("queueName", typeof<string>) :?> string
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new SendPort<'T>(queueName, config)

type ReceivePort<'T> internal (queueName, config : ConfigurationId) =
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


    interface ISerializable with
        member x.GetObjectData(info : SerializationInfo, context : StreamingContext) : unit = 
            info.AddValue("queueName", queueName, typeof<string>)
            info.AddValue("config", config, typeof<ConfigurationId>)
    
    new(info : SerializationInfo, context : StreamingContext) = 
        let queueName = info.GetValue("queueName", typeof<string>) :?> string
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new ReceivePort<'T>(queueName, config)

[<AutoSerializableAttribute(false)>]
type ChannelProvider private (config : ConfigurationId) =
    
    interface ICloudChannelProvider with
        member x.CreateUniqueContainerName() : string = guid()
        
        member x.DisposeContainer(queueName : string): Async<unit> = 
            async {
                do! ConfigurationRegistry.Resolve<ClientProvider>(config).NamespaceClient.DeleteQueueAsync(queueName)
            }
        
        // TODO : Change
        member __.Name : string = ConfigurationRegistry.Resolve<ClientProvider>(config).NamespaceClient.Address.ToString()
        
        member __.Id = ConfigurationRegistry.Resolve<ClientProvider>(config).NamespaceClient.Address.ToString()

        member __.CreateChannel<'T> (_ : string) =
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

        member this.GetChannelProviderDescriptor() : ICloudAtomProviderDescriptor = 
            let this = this :> ICloudChannelProvider
            let id = this.Id
            let name = this.Name
            let config = config
            { new ICloudAtomProviderDescriptor with
                  member x.Id: string = 
                      failwith "Not implemented yet"
                  
                  member x.Name: string = 
                      failwith "Not implemented yet"
                  
                  member x.Recover(): ICloudAtomProvider = 
                      failwith "Not implemented yet"
            }

    static member Create(config : ConfigurationId) : ICloudChannelProvider =
        new ChannelProvider(config) :> _
