namespace MBrace.Azure.Store

open System
open System.IO
open System.Threading.Tasks

open Microsoft.WindowsAzure.Storage

open MBrace.Azure.Runtime.Utilities

[<AutoOpen>]
module internal Utils =

    // TODO : merge with global runtime utils file

    let validateContainerName =
        //https://msdn.microsoft.com/en-us/library/azure/dd135715.aspx
        let letters = set {'a'..'z'}
        let nums = set { '0'..'9' }
        let valid = letters + nums + Set.singleton '-'
        fun (container : string) ->
            let isValid =
                container.Length >= 3 
                && container.Length <= 63
                && container |> Seq.forall valid.Contains
                && container |> Seq.head <> '-'
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
    let getContainer (account : CloudStorageAccount) (container : string) = 
        validateContainerName container
        let client = getBlobClient account
        client.GetContainerReference container

    /// <summary>
    ///     Creates a blob reference given account and full path.
    /// </summary>
    /// <param name="account">Cloud storage account.</param>
    /// <param name="path">Path to blob.</param>
    let getBlobRef account (path : string) = async {
        let container, blob = 
            match path.Split([|'/'; '\\'|], 2) with
            | [|c; b |] -> c, b
            | [|_|] -> failwithf "Invalid path '%s'. Top level files not allowed." path
            | _ -> failwithf "Invalid path '%s'." path
        let container = getContainer account container
        let! _ = container.CreateIfNotExistsAsync()
        return container.GetBlockBlobReference(blob)
    }