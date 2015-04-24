namespace MBrace.Azure.Runtime

open System
open MBrace.Core
open MBrace.Azure.Runtime.Primitives
open MBrace.Azure.Runtime.Utilities

[<Sealed>]
type internal FaultHandler () =

    /// Fault job : used for  job execution errors.
    /// Just apply fault exception.
    static member FaultJobAsync(job : Job, message, state : RuntimeState, fault : Exception) =
        async {
            
            state.Logger.Logf "Faulted message : Complete."
            do! state.ProcessManager.AddFaultedJob(job.ProcessInfo.Id)
            do! state.JobQueue.CompleteAsync(message)
        }

    /// Fault job : used for deserialization/assembly load errors.
    /// Set ResultCell of parent CloudTask and call CTS.
    static member FaultPickledJobAsync(job : PickledJob, message, state : RuntimeState, fault : Exception) =
        async {
            // Override ResultCell type and patch is with a FaultException
            state.Logger.Logf "SetResultUnsafe ResultCell %A" job.ResultCell
            let pk, rk = job.ResultCell
            do! ResultCell<obj>.SetResultUnsafe(job.ConfigurationId, pk, rk, new FaultException(sprintf "Failed to unpickle Job '%s'" job.JobId, fault))
            
            let parentTaskCTS = job.CancellationTokenSource
            state.Logger.Logf "Cancel CancellationTokenSource %O" parentTaskCTS
            parentTaskCTS.Cancel()
            
            if job.JobType = JobType.Root then
                state.Logger.Logf "Setting process Faulted"
                do! state.ProcessManager.SetFaulted(job.ProcessInfo.Id)
            
            state.Logger.Logf "Faulted message : Complete."
            do! state.ProcessManager.AddFaultedJob(job.ProcessInfo.Id)
            do! state.JobQueue.CompleteAsync(message)
        }

    /// Fault job : used when even getting PickledJob is not possible.
    /// Set ResultCell of process and call CTS.
    /// Set process as faulted.
    static member FaultMessageAsync(message : QueueMessage, state : RuntimeState, fault : Exception) =
        async {
            state.Logger.Logf "Getting process info"
            let! psInfo = state.ProcessManager.GetProcess(message.ProcessId)

            state.Logger.Logf "SetResultUnsafe ResultCell %A" psInfo.ResultRowKey
            let pk, rk = psInfo.Id, psInfo.ResultRowKey
            do! ResultCell<obj>.SetResultUnsafe(state.ConfigurationId, pk, rk, new FaultException(sprintf "Failed to download PickledJob.", fault))
            
            let parentTaskCTS = DistributedCancellationTokenSource.FromPath(state.ConfigurationId, psInfo.CancellationPartitionKey, psInfo.CancellationRowKey)
            state.Logger.Logf "Cancel CancellationTokenSource %O" parentTaskCTS
            parentTaskCTS.Cancel()
            
            state.Logger.Logf "Setting process Faulted"
            do! state.ProcessManager.SetFaulted(psInfo.Id)
            
            state.Logger.Logf "Faulted message : Complete."
            do! state.ProcessManager.AddFaultedJob(psInfo.Id)
            do! state.JobQueue.CompleteAsync(message)
        }
   