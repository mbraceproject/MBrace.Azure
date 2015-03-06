namespace MBrace.Azure.Runtime.Resources

open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common
open MBrace.Azure


type ResourceFactory private (configId : ConfigurationId) =
    member __.RequestCounter(count, pid) = Counter.Create(configId, count, pid)
    member __.RequestResultAggregator<'T>(count : int, pid) = ResultAggregator<'T>.Create(configId, count, pid)
    member __.RequestCancellationTokenSource(pid, ?metadata, ?parent, ?elevate) = DistributedCancellationTokenSource.Create(configId, pid, ?metadata = metadata, ?parent = parent, ?elevate = elevate)
    member __.RequestResultCell<'T>(taskId, pid) = ResultCell<'T>.Create(configId, taskId, pid)
    member __.RequestProcessLogger(pid) : MBrace.Runtime.ICloudLogger = 
        // TODO : change
        let pl = new ProcessLogger(configId, pid) 
        let lc = new LoggerCombiner()
        lc.Attach(new ConsoleLogger())
        lc.Attach(pl)
        lc :> _

    static member Create (configId : ConfigurationId) = new ResourceFactory(configId)