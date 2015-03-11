namespace MBrace.Azure.Runtime.Primitives

open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities
open MBrace.Azure
open Microsoft.WindowsAzure.Storage.Table
open MBrace.Azure.Runtime.Primitives
open MBrace.Azure.Runtime.Info

[<AutoSerializableAttribute(false)>]
type ResourceBatchRequest internal (configId : ConfigurationId, pid : string) =
    let requests = new TableBatchOperation()
    
    member this.AddResourceRequest<'T>(operation : TableResourceOperation<'T>) =
        operation.Operations |> Seq.iter requests.Add
        operation.Resource

    member this.AddResourceRequest(operation : TableResourceOperation) =
        operation.Operations |> Seq.iter requests.Add

    member this.RequestCounter(count) = 
        this.AddResourceRequest(Counter.Create(configId, count, pid))

    member this.RequestResultAggregator<'T>(count : int) = 
        this.AddResourceRequest(ResultAggregator<'T>.Create(configId, count, pid))

    member this.RequestResultCell<'T>(taskId) = 
        this.AddResourceRequest(ResultCell<'T>.Create(configId, taskId, pid))

    member this.CommitAsync() =
        async {
            do! Table.batch configId configId.RuntimeTable requests
        }

[<AutoSerializableAttribute(false)>]
type ResourceFactory private (configId : ConfigurationId) =
    member __.GetResourceBatchForProcess(pid) = new ResourceBatchRequest(configId, pid)
    // CTS cannot be a part of a BatchResourceRequest due to elevation...
    member __.RequestCancellationTokenSource(pid, ?metadata, ?parent, ?elevate) = 
        DistributedCancellationTokenSource.Create(configId, pid, ?metadata = metadata, ?parent = parent, ?elevate = elevate)

    member this.RequestProcessLogger(pid) : MBrace.Runtime.ICloudLogger = 
        let pl = new ProcessLogger(configId, pid) 
        let lc = new LoggerCombiner()
        lc.Attach(new ConsoleLogger())
        lc.Attach(pl)
        lc :> _
    
    static member Create (configId : ConfigurationId) = new ResourceFactory(configId)