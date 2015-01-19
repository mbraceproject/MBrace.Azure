namespace MBrace.Azure.Client

#nowarn "0444"

open MBrace.Store
open MBrace.Continuation
open MBrace.Azure.Runtime
open MBrace.Azure.Store
open MBrace
open MBrace.Azure.Runtime.Resources
open System.IO
open MBrace.Runtime.Store

///// Atom methods for MBrace.
//type CloudAtomProvider internal (registry : ResourceRegistry) =
//    let toAsync (wf : Cloud<'T>) : Async<'T> = Cloud.ToAsync(wf, registry)
//    
//    /// <summary>
//    ///     Creates a new cloud atom instance with given value.
//    /// </summary>
//    /// <param name="initial">Initial value.</param>
//    member __.New<'T>(initial : 'T) : Async<ICloudAtom<'T>> =  MBrace.CloudAtom.New(initial) |> toAsync
//       
//    /// <summary>
//    ///     Dereferences a cloud atom.
//    /// </summary>
//    /// <param name="atom">Atom instance.</param>
//    member __.Read(atom : ICloudAtom<'T>) : Async<'T> = MBrace.CloudAtom.Read(atom) |> toAsync
//
//    /// <summary>
//    ///     Atomically updates the contained value.
//    /// </summary>
//    /// <param name="updater">value updating function.</param>
//    /// <param name="atom">Atom instance to be updated.</param>
//    member __.Update (updateF : 'T -> 'T) (atom : ICloudAtom<'T>) : Async<unit> = atom.Update updateF
//
//    /// <summary>
//    ///     Forces the contained value to provided argument.
//    /// </summary>
//    /// <param name="value">Value to be set.</param>
//    /// <param name="atom">Atom instance to be updated.</param>
//    member __.Force (value : 'T) (atom : ICloudAtom<'T>) : Async<unit> = atom.Force value
//
//    /// <summary>
//    ///     Transactionally updates the contained value.
//    /// </summary>
//    /// <param name="trasactF"></param>
//    /// <param name="atom"></param>
//    member __.Transact (trasactF : 'T -> 'R * 'T) (atom : ICloudAtom<'T>) : Async<'R> = atom.Transact trasactF
//
//    /// <summary>
//    ///     Deletes the provided atom instance from store.
//    /// </summary>
//    /// <param name="atom">Atom instance to be deleted.</param>
//    member __.Delete (atom : ICloudAtom<'T>) = Cloud.Dispose atom |> toAsync
//
//    /// <summary>
//    ///     Checks if value is supported by current table store.
//    /// </summary>
//    /// <param name="value">Value to be checked.</param>
//    member __.IsSupportedValue(value : 'T) = MBrace.CloudAtom.IsSupportedValue value |> toAsync
//
///// Channel methods for MBrace.
//type CloudChannelProvider internal (registry : ResourceRegistry) =
//    let toAsync (wf : Cloud<'T>) : Async<'T> = Cloud.ToAsync(wf, registry)
//
//    /// Creates a new channel instance.
//    member __.New<'T>() = MBrace.CloudChannel.New<'T>() |> toAsync
//
//    /// <summary>
//    ///     Send message to the channel.
//    /// </summary>
//    /// <param name="message">Message to send.</param>
//    /// <param name="channel">Target channel.</param>
//    member __.Send<'T> (message : 'T) (channel : ISendPort<'T>) = MBrace.CloudChannel.Send<'T> message channel |> toAsync
//
//    /// <summary>
//    ///     Receive message from channel.
//    /// </summary>
//    /// <param name="channel">Source channel.</param>
//    /// <param name="timeout">Timeout in milliseconds.</param>
//    member __.Receive<'T> (channel : IReceivePort<'T>, ?timeout : int) =  MBrace.CloudChannel.Receive(channel, ?timeout = timeout) |> toAsync
//
//
///// Collection of file store operations
//type CloudFileProvider internal (registry : ResourceRegistry) =
//    let toAsync (wf : Cloud<'T>) : Async<'T> = Cloud.ToAsync(wf, registry)
//    
//    /// <summary>
//    ///     Returns the directory name for given path.
//    /// </summary>
//    /// <param name="path">Input file path.</param>
//    member __.GetDirectoryName(path : string) = MBrace.FileStore.GetDirectoryName(path) |> toAsync
//
//    /// <summary>
//    ///     Returns the file name for given path.
//    /// </summary>
//    /// <param name="path">Input file path.</param>
//    member __.GetFileName(path : string) = MBrace.FileStore.GetFileName(path) |> toAsync
//
//    /// <summary>
//    ///     Combines two strings into one path.
//    /// </summary>
//    /// <param name="path1">First path.</param>
//    /// <param name="path2">Second path.</param>
//    member __.Combine(path1 : string, path2 : string) = MBrace.FileStore.Combine(path1, path2) |> toAsync
//
//    /// <summary>
//    ///     Combines an array of paths into a path.
//    /// </summary>
//    /// <param name="paths">Strings to be combined.</param>
//    member __.Combine(paths : string []) = MBrace.FileStore.Combine(paths) |> toAsync
//
//    /// <summary>
//    ///     Combines a collection of file names with provided directory prefix.
//    /// </summary>
//    /// <param name="directory">Directory prefix path.</param>
//    /// <param name="fileNames">File names to be combined.</param>
//    member __.Combine(directory : string, fileNames : seq<string>) = MBrace.FileStore.Combine(directory, fileNames) |> toAsync
//
//    /// <summary>
//    /// Returns current ICloudFileStore instance.
//    /// </summary>
//    member __.Current : Async<ICloudFileStore> = FileStore.Current |> toAsync
//                   
//    /// Generates a random, uniquely specified path to directory
//    member __.CreateUniqueDirectoryPath() = MBrace.FileStore.CreateUniqueDirectoryPath() |> toAsync
//    
/// Provides methods for interacting with cloud storage.
[<Sealed; AutoSerializable(false)>]
type StoreClient internal (config : Configuration) =
    
    do Configuration.Activate(config)

    let inMemoryCache = InMemoryCache.Create(physicalMemoryLimitPercentage = 20)
    let cache = FileSystemStore.CreateUniqueLocal()
    let mutable storeProvider = BlobStore.Create(config.StorageConnectionString) :> ICloudFileStore
    let mutable atomProvider = 
        { new AtomProvider(config.StorageConnectionString, Configuration.Serializer) with
            override __.ComputeSize(value : 'T) = Configuration.Pickler.ComputeSize(value) } :> ICloudAtomProvider
    let mutable channelProvider = ChannelProvider.Create(config.ServiceBusConnectionString, Configuration.Serializer)
    
    let mutable defaultStoreContainer = config.DefaultTableOrContainer
    let mutable defaultAtomContainer = config.DefaultTableOrContainer
    let mutable defaultChannelContainer = ""

    let resources () = 
        resource { 
            yield { FileStore = FileStoreCache.Create(storeProvider, cache);
                    DefaultDirectory = defaultStoreContainer;
                    Cache = inMemoryCache;
                    Serializer = Configuration.Serializer
                   }
            yield { AtomProvider = atomProvider; DefaultContainer = defaultAtomContainer }
            yield { ChannelProvider = channelProvider; DefaultContainer = defaultChannelContainer } 
        }

    member internal __.StoreResources = resources()

    member __.DefaultStoreContainer with get () = defaultStoreContainer and set c = defaultStoreContainer <- c
    member __.DefaultAtomContainer with get () = defaultAtomContainer and set c = defaultAtomContainer <- c
    member __.DefaultChannelContainer with get () = defaultChannelContainer and set c = defaultChannelContainer <- c

    member __.StoreProvider
        with get () = FileStoreCache.Create(storeProvider, cache) :> ICloudFileStore
        and set(store : ICloudFileStore) = storeProvider <- store
    member __.AtomProvider
        with get () = atomProvider
        and set(atom : ICloudAtomProvider) = atomProvider <- atom
    member __.ChannelProvider 
        with get () = channelProvider
        and set(channel : ICloudChannelProvider) = channelProvider <- channel 

//    member __.CloudAtom with get () = new CloudAtomProvider(resources())
//    member __.CloudChannel with get () = new CloudChannelProvider(resources())
//    member __.CloudFile with get () = new CloudFileProvider(resources())

    static member Create(config : Configuration) = new StoreClient(config)