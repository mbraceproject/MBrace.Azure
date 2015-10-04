namespace MBrace.Azure.Runtime

open System
open System.Runtime.Serialization

open Microsoft.WindowsAzure.Storage.Table

open Nessos.FsPickler

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Components
open MBrace.Azure
open MBrace.Azure.Runtime.Utilities

[<Sealed; DataContract>]
type CloudProcessManager private (config : ClusterId, logger : ISystemLogger) =

    let [<DataMember(Name="config")>] config = config
    let [<DataMember(Name="logger")>] logger = logger

    interface ICloudProcessManager with
        member this.ClearProcess(taskId: string): Async<unit> = async {
            let record = new CloudProcessRecord(taskId)
            record.ETag <- "*"
            return! Table.delete config.StorageAccount config.RuntimeTable record // TODO : perform full cleanup?
        }
        
        member this.ClearAllProcesses(): Async<unit> = async {
            let! records = Table.queryPK<CloudProcessRecord> config.StorageAccount config.RuntimeTable CloudProcessRecord.DefaultPartitionKey
            return! Table.deleteBatch config.StorageAccount config.RuntimeTable records
        }
        
        member this.StartProcess(info: CloudProcessInfo): Async<ICloudProcessEntry> = async {
            let taskId = guid()
            logger.LogInfof "task:%A : creating task" taskId
            let record = CloudProcessRecord.CreateNew(taskId, info)
            let! _record = Table.insertOrReplace config.StorageAccount config.RuntimeTable record
            let tcs = new CloudProcessEntry(config, taskId, info)
            logger.LogInfof "%A : task created" tcs
            return tcs :> ICloudProcessEntry
        }
        
        member this.GetAllProcesses(): Async<ICloudProcessEntry []> = async {
            let! records = Table.queryPK<CloudProcessRecord> config.StorageAccount config.RuntimeTable CloudProcessRecord.DefaultPartitionKey
            return records |> Seq.map(fun r -> new CloudProcessEntry(config, r.Id, r.ToCloudProcessInfo()) :> ICloudProcessEntry) |> Seq.toArray
        }
        
        member this.TryGetProcessById(taskId: string): Async<ICloudProcessEntry option> = async {
            let! record = Table.read<CloudProcessRecord> config.StorageAccount config.RuntimeTable CloudProcessRecord.DefaultPartitionKey taskId
            if record = null then return None else return Some(new CloudProcessEntry(config, taskId, record.ToCloudProcessInfo()) :> ICloudProcessEntry)
        }

    static member Create(config : ClusterId, logger) = new CloudProcessManager(config, logger)