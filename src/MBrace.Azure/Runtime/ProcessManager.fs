namespace MBrace.Azure.Runtime

open System
open System.Runtime.Serialization

open Microsoft.WindowsAzure.Storage.Table

open MBrace.FsPickler

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Components
open MBrace.Azure
open MBrace.Azure.Runtime.Utilities

[<Sealed; AutoSerializable(false)>]
type CloudProcessManager private (clusterId : ClusterId, logger : ISystemLogger) =

    interface ICloudProcessManager with
        member this.ClearProcess(taskId: string): Async<unit> = async {
            let record = new CloudProcessRecord(taskId)
            record.ToCloudProcessInfo().CancellationTokenSource.Cancel()
            record.ETag <- "*"
            return! Table.delete clusterId.StorageAccount clusterId.RuntimeTable record // TODO : perform full cleanup?
        }
        
        member this.ClearAllProcesses(): Async<unit> = async {
            let! records = Table.queryPK<CloudProcessRecord> clusterId.StorageAccount clusterId.RuntimeTable CloudProcessRecord.DefaultPartitionKey
            for r in records do r.ToCloudProcessInfo().CancellationTokenSource.Cancel()
            return! Table.deleteBatch clusterId.StorageAccount clusterId.RuntimeTable records
        }
        
        member this.StartProcess(info: CloudProcessInfo): Async<ICloudProcessEntry> = async {
            let taskId = guid()
            logger.LogInfof "Creating cloud process %A" taskId
            let record = CloudProcessRecord.CreateNew(taskId, info)
            let! _record = Table.insertOrReplace clusterId.StorageAccount clusterId.RuntimeTable record
            let tcs = new CloudProcessEntry(clusterId, taskId, info)
            return tcs :> ICloudProcessEntry
        }
        
        member this.GetAllProcesses(): Async<ICloudProcessEntry []> = async {
            let! records = Table.queryPK<CloudProcessRecord> clusterId.StorageAccount clusterId.RuntimeTable CloudProcessRecord.DefaultPartitionKey
            return records |> Seq.map(fun r -> new CloudProcessEntry(clusterId, r.Id, r.ToCloudProcessInfo()) :> ICloudProcessEntry) |> Seq.toArray
        }
        
        member this.TryGetProcessById(taskId: string): Async<ICloudProcessEntry option> = async {
            let! record = Table.read<CloudProcessRecord> clusterId.StorageAccount clusterId.RuntimeTable CloudProcessRecord.DefaultPartitionKey taskId
            if record = null then return None else return Some(new CloudProcessEntry(clusterId, taskId, record.ToCloudProcessInfo()) :> ICloudProcessEntry)
        }

    static member Create(clusterId : ClusterId, logger: ISystemLogger) = new CloudProcessManager(clusterId, logger)