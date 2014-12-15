namespace Nessos.MBrace.Azure.Client

#nowarn "0444"

open Nessos.MBrace.Store
open Nessos.MBrace.Continuation
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Store
open Nessos.MBrace
open Nessos.MBrace.Azure.Runtime.Resources
open System.IO

/// Atom methods for MBrace.
type CloudAtom internal (registry : ResourceRegistry) =
    let toAsync (wf : Cloud<'T>) : Async<'T> = Cloud.ToAsync(wf, registry)
    
    /// <summary>
    ///     Creates a new cloud atom instance with given value.
    /// </summary>
    /// <param name="initial">Initial value.</param>
    member __.New<'T>(initial : 'T) : Async<ICloudAtom<'T>> =  Nessos.MBrace.CloudAtom.New(initial) |> toAsync
       
    /// <summary>
    ///     Dereferences a cloud atom.
    /// </summary>
    /// <param name="atom">Atom instance.</param>
    member __.Read(atom : ICloudAtom<'T>) : Async<'T> = Nessos.MBrace.CloudAtom.Read(atom) |> toAsync

    /// <summary>
    ///     Atomically updates the contained value.
    /// </summary>
    /// <param name="updater">value updating function.</param>
    /// <param name="atom">Atom instance to be updated.</param>
    member __.Update (updateF : 'T -> 'T) (atom : ICloudAtom<'T>) : Async<unit> = atom.Update updateF

    /// <summary>
    ///     Forces the contained value to provided argument.
    /// </summary>
    /// <param name="value">Value to be set.</param>
    /// <param name="atom">Atom instance to be updated.</param>
    member __.Force (value : 'T) (atom : ICloudAtom<'T>) : Async<unit> = atom.Force value

    /// <summary>
    ///     Transactionally updates the contained value.
    /// </summary>
    /// <param name="trasactF"></param>
    /// <param name="atom"></param>
    member __.Transact (trasactF : 'T -> 'R * 'T) (atom : ICloudAtom<'T>) : Async<'R> = atom.Transact trasactF

    /// <summary>
    ///     Deletes the provided atom instance from store.
    /// </summary>
    /// <param name="atom">Atom instance to be deleted.</param>
    member __.Delete (atom : ICloudAtom<'T>) = Cloud.Dispose atom |> toAsync

    /// <summary>
    ///     Checks if value is supported by current table store.
    /// </summary>
    /// <param name="value">Value to be checked.</param>
    member __.IsSupportedValue(value : 'T) = Nessos.MBrace.CloudAtom.IsSupportedValue value |> toAsync

/// Channel methods for MBrace.
type CloudChannel internal (registry : ResourceRegistry) =
    let toAsync (wf : Cloud<'T>) : Async<'T> = Cloud.ToAsync(wf, registry)

    /// Creates a new channel instance.
    member __.New<'T>() = Nessos.MBrace.CloudChannel.New<'T>() |> toAsync

    /// <summary>
    ///     Send message to the channel.
    /// </summary>
    /// <param name="message">Message to send.</param>
    /// <param name="channel">Target channel.</param>
    member __.Send<'T> (message : 'T) (channel : ISendPort<'T>) = Nessos.MBrace.CloudChannel.Send<'T> message channel |> toAsync

    /// <summary>
    ///     Receive message from channel.
    /// </summary>
    /// <param name="channel">Source channel.</param>
    /// <param name="timeout">Timeout in milliseconds.</param>
    member __.Receive<'T> (channel : IReceivePort<'T>, ?timeout : int) =  Nessos.MBrace.CloudChannel.Receive(channel, ?timeout = timeout) |> toAsync


/// Collection of file store operations
type CloudFileStore internal (registry : ResourceRegistry) =
    let toAsync (wf : Cloud<'T>) : Async<'T> = Cloud.ToAsync(wf, registry)

    /// <summary>
    ///     Returns the directory name for given path.
    /// </summary>
    /// <param name="path">Input file path.</param>
    member __.GetDirectoryName(path : string) = Nessos.MBrace.FileStore.GetDirectoryName(path) |> toAsync

    /// <summary>
    ///     Returns the file name for given path.
    /// </summary>
    /// <param name="path">Input file path.</param>
    member __.GetFileName(path : string) = Nessos.MBrace.FileStore.GetFileName(path) |> toAsync

    /// <summary>
    ///     Combines two strings into one path.
    /// </summary>
    /// <param name="path1">First path.</param>
    /// <param name="path2">Second path.</param>
    member __.Combine(path1 : string, path2 : string) = Nessos.MBrace.FileStore.Combine(path1, path2) |> toAsync

    /// <summary>
    ///     Combines an array of paths into a path.
    /// </summary>
    /// <param name="paths">Strings to be combined.</param>
    member __.Combine(paths : string []) = Nessos.MBrace.FileStore.Combine(paths) |> toAsync

    /// <summary>
    ///     Combines a collection of file names with provided directory prefix.
    /// </summary>
    /// <param name="directory">Directory prefix path.</param>
    /// <param name="fileNames">File names to be combined.</param>
    member __.Combine(directory : string, fileNames : seq<string>) = Nessos.MBrace.FileStore.Combine(directory, fileNames) |> toAsync

    /// <summary>
    ///     Gets the size of provided file, in bytes.
    /// </summary>
    /// <param name="path">Path to file.</param>
    member __.GetFileSize(path : string) = Nessos.MBrace.FileStore.GetFileSize(path) |> toAsync

    /// Generates a random, uniquely specified path to directory
    member __.CreateUniqueDirectoryPath() = Nessos.MBrace.FileStore.CreateUniqueDirectoryPath() |> toAsync

    /// <summary>
    ///     Checks if file exists in store.
    /// </summary>
    /// <param name="path">Path to file.</param>
    member __.FileExists(path : string) = Nessos.MBrace.FileStore.FileExists(path) |> toAsync

    /// <summary>
    ///     Gets all files that exist in given container.
    /// </summary>
    /// <param name="directory">Path to directory. Defaults to the root directory.</param>
    member __.EnumerateFiles(?directory : string) = Nessos.MBrace.FileStore.EnumerateFiles(?directory = directory) |> toAsync

    /// <summary>
    ///     Deletes file in given path.
    /// </summary>
    /// <param name="path">File path.</param>
    member __.DeleteFile(path : string) = Nessos.MBrace.FileStore.DeleteFile(path) |> toAsync

    /// <summary>
    ///     Checks if directory exists in given path
    /// </summary>
    /// <param name="directory">Path to directory.</param>
    member __.DirectoryExists(directory : string) = Nessos.MBrace.FileStore.DirectoryExists(directory) |> toAsync

    /// <summary>
    ///     Creates a new directory in store.
    /// </summary>
    /// <param name="directory">Path to directory. Defaults to randomly generated directory.</param>
    member __.CreateDirectory(?directory : string) = Nessos.MBrace.FileStore.CreateDirectory(?directory = directory) |> toAsync

    /// <summary>
    ///     Deletes directory from store.
    /// </summary>
    /// <param name="directory">Directory to be deleted.</param>
    /// <param name="recursiveDelete">Delete recursively. Defaults to false.</param>
    member __.DeleteDirectory(directory : string, ?recursiveDelete : bool) = Nessos.MBrace.FileStore.DeleteDirectory(directory, ?recursiveDelete = recursiveDelete) |> toAsync

    /// <summary>
    ///     Enumerates all directories in directory.
    /// </summary>
    /// <param name="directory">Directory to be enumerated. Defaults to root directory.</param>
    member __.EnumerateDirectories(?directory : string) = Nessos.MBrace.FileStore.EnumerateDirectories(?directory = directory) |> toAsync

    /// <summary>
    ///     Creates a new file in store with provided serializer function.
    /// </summary>
    /// <param name="serializer">Serializer function.</param>
    /// <param name="path">Path to file. Defaults to auto-generated path.</param>
    member __.CreateFile(serializer : Stream -> Async<unit>, ?path : string) = Nessos.MBrace.FileStore.CreateFile(serializer, ?path = path) |> toAsync

    /// <summary>
    ///     Creates a new file in store with provided serializer function.
    /// </summary>
    /// <param name="serializer">Serializer function.</param>
    /// <param name="directory">Containing directory.</param>
    /// <param name="fileName">File name.</param>
    member __.CreateFile(serializer : Stream -> Async<unit>, directory : string, fileName : string) = 
        Nessos.MBrace.FileStore.CreateFile(serializer, directory, fileName) |> toAsync

    /// <summary>
    ///     Reads file in store with provided deserializer function.
    /// </summary>
    /// <param name="deserializer">Deserializer function.</param>
    /// <param name="path">Path to file.</param>
    member __.ReadFile<'T>(deserializer : Stream -> Async<'T>, path : string) = Nessos.MBrace.FileStore.ReadFile(deserializer, path) |> toAsync

/// Provides methods for interacting with cloud storage.
[<Sealed; AutoSerializable(false)>]
type StoreClient internal (config : Configuration) =
    let mutable storeProvider   = BlobStore(config.StorageConnectionString) :> ICloudFileStore
    let mutable atomProvider    = AtomProvider.Create(config.ConfigurationId)
    let mutable channelProvider = ChannelProvider.Create(config.ConfigurationId)

    let mutable defaultStoreContainer = config.DefaultTableOrContainer
    let mutable defaultAtomContainer = config.DefaultTableOrContainer
    let mutable defaultChannelContainer = ""

    let resources () = 
        resource { 
            yield { FileStore = storeProvider; DefaultDirectory = defaultStoreContainer }
            yield { AtomProvider = atomProvider; DefaultContainer = defaultAtomContainer }
            yield { ChannelProvider = channelProvider; DefaultContainer = defaultChannelContainer } 
        }

    member __.DefaultStoreContainer with get () = defaultStoreContainer and set c = defaultStoreContainer <- c
    member __.DefaultAtomContainer with get () = defaultAtomContainer and set c = defaultAtomContainer <- c
    member __.DefaultChannelContainer with get () = defaultChannelContainer and set c = defaultChannelContainer <- c

    member __.StoreProvider
        with get () = storeProvider
        and set(store : ICloudFileStore) = storeProvider <- store
    member __.AtomProvider
        with get () = atomProvider
        and set(atom : ICloudAtomProvider) = atomProvider <- atom
    member __.ChannelProvider 
        with get () = channelProvider
        and set(channel : ICloudChannelProvider) = channelProvider <- channel 

    member __.CloudAtom with get () = new CloudAtom(resources())
    member __.CloudChannel with get () = new CloudAtom(resources())
    member __.CloudFileStore with get () = new CloudFileStore(resources())

    static member Create(config : Configuration) = new StoreClient(config)