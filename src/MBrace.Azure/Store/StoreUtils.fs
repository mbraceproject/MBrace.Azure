namespace MBrace.Azure.Store

open System
open System.IO
open System.Threading.Tasks

open Microsoft.WindowsAzure.Storage

open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities

// TODO : merge with global runtime utils file

[<AutoOpen>]
module internal Utils =
    
    let rootPath = "/"
    let rootPathAlt = @"\"
    let delims = [|'/'; '\\'|]

    let isPathRooted (path : string) =
        path.StartsWith rootPath || path.StartsWith rootPathAlt

    let normalizePath (path : string) =
        let path = if isPathRooted path then path else Path.Combine(rootPath, path)
        path.Replace('\\', '/')

    let ensureRooted (path : string) =
        if isPathRooted path then path else raise <| FormatException(sprintf "Invalid path %A. Paths should start with '/' or '\\'." path)

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
            let path = ensureRooted path    
            let xs = path.Split(delims, 2, StringSplitOptions.RemoveEmptyEntries)
            match xs with
            | [||] -> { Container = Root; SubDirectory = None }
            | [|c|] -> { Container = Container c; SubDirectory = None }
            | [|c; x|] -> { Container = Container c; SubDirectory = Some x }
            | _ -> failwith "Invalid store path %A" path

    /// Represents a full path to a blob.
    type StorePath =
        {
            Container : Container
            BlobName : string 
        }

        static member Parse(path : string) =
            let path = ensureRooted path    
            let xs = path.Split(delims, 2, StringSplitOptions.RemoveEmptyEntries)
            match xs with
            | [|x|] -> { Container = Root; BlobName = x }
            | [|c; x|] -> { Container = Container c; BlobName = x }
            | _ -> failwith "Invalid store path %A" path

    /// <summary>
    ///     Creates a blob storage container reference given account and container name
    /// </summary>
    /// <param name="account">Storage account instance.</param>
    /// <param name="container">Container name</param>
    let getContainerReference (account : AzureStorageAccount) (container : Container) = 
        let client = account.BlobClient
        match container with
        | Root -> 
            let root = client.GetRootContainerReference()
            let _ = root.CreateIfNotExists()
            root
        | Container c ->
            Validate.containerName c
            client.GetContainerReference c

    /// <summary>
    ///     Creates a blob reference given account and full path.
    /// </summary>
    /// <param name="account">Cloud storage account.</param>
    /// <param name="path">Path to blob.</param>
    let getBlobReference account (fullPath : string) = async {
        let path = StorePath.Parse fullPath
        let container = getContainerReference account path.Container
        let _ =
            match path.Container with
            | Container _ -> container.CreateIfNotExists()
            | Root -> true
        return container.GetBlockBlobReference(path.BlobName)
    }