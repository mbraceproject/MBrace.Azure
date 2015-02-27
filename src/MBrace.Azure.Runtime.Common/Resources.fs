namespace MBrace.Azure.Runtime.Resources

open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common
open MBrace.Azure


type ResourceFactory private (configId : ConfigurationId) =
    member __.RequestCounter(count) = Counter.Create(configId, count)
    member __.RequestResultAggregator<'T>(count : int) = ResultAggregator<'T>.Create(configId, count)
    member __.RequestCancellationTokenSource(?metadata, ?parent) = DistributedCancellationTokenSource.Create(configId, ?metadata = metadata, ?parent = parent)
    member __.RequestResultCell<'T>(taskId) = ResultCell<'T>.Create(configId, taskId)
    member __.RequestProcessLogger(pid) : MBrace.Runtime.ICloudLogger = 
        // TODO : change
        let pl = new ProcessLogger(configId, pid) 
        let lc = new LoggerCombiner()
        lc.Attach(new ConsoleLogger())
        lc.Attach(pl)
        lc :> _

    static member Create (configId : ConfigurationId) = new ResourceFactory(configId)