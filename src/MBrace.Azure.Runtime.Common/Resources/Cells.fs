namespace MBrace.Azure.Runtime.Resources

open System
open System.Runtime.Serialization
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Common
open System.IO
open MBrace.Azure


type Blob<'T> internal (config : ConfigurationId, prefix, filename) = 
    member __.GetValue() : Async<'T> = 
        async { 
            let container = ConfigurationRegistry.Resolve<ClientProvider>(config).BlobClient.GetContainerReference(config.RuntimeContainer)
            use! s = container.GetBlockBlobReference(sprintf "%s/%s" prefix filename).OpenReadAsync()
            return Configuration.Pickler.Deserialize<'T>(s) 
        }
    
    member __.Path = sprintf "%s/%s" prefix filename
    
    interface ISerializable with
        member x.GetObjectData(info: SerializationInfo, _: StreamingContext): unit = 
            info.AddValue("prefix", prefix, typeof<string>)
            info.AddValue("filename", filename, typeof<string>)
            info.AddValue("config", config, typeof<ConfigurationId>)

    new(info: SerializationInfo, _: StreamingContext) =
        let filename = info.GetValue("filename", typeof<string>) :?> string
        let prefix = info.GetValue("prefix", typeof<string>) :?> string
        let config = info.GetValue("config", typeof<ConfigurationId>) :?> ConfigurationId
        new Blob<'T>(config, prefix, filename)

    static member FromPath(config : ConfigurationId, path : string) = 
        let p = path.Split('/')
        Blob<'T>.FromPath(config, p.[0], p.[1])
    static member FromPath(config : ConfigurationId, prefix, file) = 
        new Blob<'T>(config, prefix, file)
    static member CreateIfNotExists(config, prefix, filename : string, f : unit -> 'T) = 
        async { 
            let c = ConfigurationRegistry.Resolve<ClientProvider>(config).BlobClient.GetContainerReference(config.RuntimeContainer)
            let! _ = c.CreateIfNotExistsAsync()
            let b = c.GetBlockBlobReference(sprintf "%s/%s" prefix filename)
            let! exists = b.ExistsAsync()
            if not exists then
                let tmpPath = Path.GetTempFileName()
                let tmp = File.Create(tmpPath)
                Configuration.Pickler.Serialize<'T>(tmp, f(), leaveOpen = true)
                tmp.Position <- 0L
                do! b.UploadFromStreamAsync(tmp) 
                tmp.Dispose()
                File.Delete(tmpPath)
                return new Blob<'T>(config, prefix, filename)
            else
                return Blob.FromPath(config, prefix, filename)
        }