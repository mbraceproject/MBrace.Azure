namespace MBrace.Azure.Store

open System
open System.IO
open System.Threading.Tasks

open Microsoft.WindowsAzure.Storage

open MBrace.Azure.Runtime.Utilities



[<AutoOpen>]
module internal Utils =
    
    /// Azure blob container.
    type Container =
        | Root
        | Container of string
    
    /// Represents a 'directory' in blob storage.
    type StoreDirectory =
        {
            Container : Container
            SubDirectory : string option
        }

        static member Parse(path : string) =
            let xs = path.Split([| '/'; '\\' |], 2)
            match xs with
            | [|c|] when c = "" -> { Container = Root; SubDirectory = None }
            | [|c|] -> { Container = Container c; SubDirectory = None }
            | [|c; x|] -> { Container = Container c; SubDirectory = Some x }
            | _ -> failwith "Invalid store path %A" path

    /// Represents a full path to a blob.
    type StorePath =
        {
            Container : Container
            RelativePath : string 
        }

        static member Parse(path : string) =
            let xs = path.Split([| '/'; '\\' |], 2)
            match xs with
            | [|x|] -> { Container = Root; RelativePath = x }
            | [|c; x|] -> { Container = Container c; RelativePath = x }
            | _ -> failwith "Invalid store path %A" path

    // TODO : merge with global runtime utils file

    let validateContainerName =
        //https://msdn.microsoft.com/en-us/library/azure/dd135715.aspx
        let letters = set {'a'..'z'}
        let nums = set { '0'..'9' }
        let valid = letters + nums + Set.singleton '-'
        fun (container : string) ->
            let isValid =
                container = ""
                || (container.Length >= 3 
                    && container.Length <= 63
                    && container |> Seq.forall valid.Contains
                    && container |> Seq.head <> '-')
            if not isValid then failwithf "Invalid container '%s'" container
            

    /// Creates a Microsoft.WindowsAzure.Storage.BlobClient instance given a cloud storage account
    let getBlobClient (account : CloudStorageAccount) =
        let client = account.CreateCloudBlobClient()
        client.DefaultRequestOptions.ServerTimeout <- Nullable(TimeSpan.FromMinutes(40.))
        client.DefaultRequestOptions.ParallelOperationThreadCount <- System.Nullable(min 64 <| 4 * System.Environment.ProcessorCount)
        client.DefaultRequestOptions.SingleBlobUploadThresholdInBytes <- System.Nullable(4L * 1024L * 1024L) // possible ranges: 1..64MB, default 32MB
        
        client

    /// <summary>
    ///     Creates a blob storage container reference given account and container name
    /// </summary>
    /// <param name="account">Storage account instance.</param>
    /// <param name="container">Container name</param>
    let getContainerReference (account : CloudStorageAccount) (container : Container) = 
        let client = getBlobClient account
        match container with
        | Root -> client.GetRootContainerReference()
        | Container c ->
            validateContainerName c
            client.GetContainerReference c

    /// <summary>
    ///     Creates a blob reference given account and full path.
    /// </summary>
    /// <param name="account">Cloud storage account.</param>
    /// <param name="path">Path to blob.</param>
    let getBlobReference account (fullPath : string) = async {
        let path = StorePath.Parse fullPath
        let container = getContainerReference account path.Container
        let _ = container.CreateIfNotExists()
        return container.GetBlockBlobReference(path.RelativePath)
    }