namespace MBrace.Azure.Runtime.Resources

open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common


type ResourceFactory private (configId : ConfigurationId) =
    member __.RequestCounter(container, count) = Counter.Create(configId, container, count)
    member __.RequestResultAggregator<'T>(container, count : int) = ResultAggregator<'T>.Create(configId, container, count)
    member __.RequestCancellationTokenSource(container, ?metadata, ?parent) = DistributedCancellationTokenSource.Create(configId, container, ?metadata = metadata, ?parent = parent)
    member __.RequestResultCell<'T>(taskId, container) = ResultCell<'T>.Create(configId, taskId, container)
    member __.RequestProcessLogger(container, pid) : MBrace.Runtime.ICloudLogger = 
        // TODO : change
        let pl = new ProcessLogger(configId, container, pid) 
        let lc = new LoggerCombiner()
        lc.Attach(new ConsoleLogger())
        lc.Attach(pl)
        lc :> _

    static member Create (configId : ConfigurationId) = new ResourceFactory(configId)