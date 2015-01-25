namespace MBrace.Azure.Runtime.Resources

open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common


type ResourceFactory private (config : Configuration) =
    member __.RequestCounter(container, count) = Counter.Create(config.ConfigurationId, container, count)
    member __.RequestResultAggregator<'T>(container, count : int) = ResultAggregator<'T>.Create(config.ConfigurationId, container, count)
    member __.RequestCancellationTokenSource(container, ?parent) = DistributedCancellationTokenSource.Create(config.ConfigurationId, container, ?parent = parent)
    member __.RequestResultCell<'T>(container) = ResultCell<Result<'T>>.Create(config.ConfigurationId, container)
    member __.RequestProcessLogger(container, pid) = 
        // TODO : change
        let logger = new ProcessLogger(config.ConfigurationId, container, ProcessLog(id = pid)) 
        logger.Attach(new ConsoleLogger())
        logger 

    static member Create (config : Configuration) = new ResourceFactory(config)