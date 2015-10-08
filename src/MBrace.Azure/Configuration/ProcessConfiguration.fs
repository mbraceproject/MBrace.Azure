namespace MBrace.Azure.Runtime

open System
open System.IO
open System.Net

open Nessos.FsPickler
open Nessos.FsPickler.Json

open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Store

/// Configuration registry for current process.
type ProcessConfiguration private () =
    static let isInitialized = ref false
    static let mutable objectCache = Unchecked.defaultof<InMemoryCache>
    static let mutable localFileStore = Unchecked.defaultof<FileSystemStore>
    static let mutable workingDirectory = Unchecked.defaultof<string>
    static let mutable jsonSerializer = Unchecked.defaultof<JsonSerializer>
    static let mutable version = Unchecked.defaultof<Version>

    static let checkInitialized () =
        if not isInitialized.Value then
            invalidOp "Azure configuration has not been initialized in the current process."

    static let initGlobalState workDir populateDirs isClientInstance =
        lock isInitialized (fun () ->
            if not isInitialized.Value then
                do ServicePointManager.DefaultConnectionLimit <- 512
                do ServicePointManager.Expect100Continue <- false
                do ServicePointManager.UseNagleAlgorithm <- false

                let _ = System.Threading.ThreadPool.SetMinThreads(256, 256)
                workingDirectory <- match workDir with Some w -> w | None -> WorkingDirectory.GetDefaultWorkingDirectoryForProcess(prefix = "mbrace.azure")
                let _ = WorkingDirectory.CreateWorkingDirectory(workingDirectory, cleanup = populateDirs)
                let vagabondDir = Path.Combine(workingDirectory, "vagabond")
                if populateDirs then ignore <| WorkingDirectory.CreateWorkingDirectory(vagabondDir, cleanup = false)
                VagabondRegistry.Initialize(vagabondDir, isClientSession = isClientInstance)
                objectCache <- InMemoryCache.Create(name = "MBrace.Azure object cache")
                localFileStore <- FileSystemStore.Create(rootPath = Path.Combine(workingDirectory, "localStore"), create = populateDirs)
                jsonSerializer <- FsPickler.CreateJsonSerializer(indent = false, omitHeader = true, typeConverter = VagabondRegistry.Instance.TypeConverter)
                version <- typeof<ProcessConfiguration>.Assembly.GetName().Version
                isInitialized := true
        )

    /// Ensure that global configuration object has been initialized
    static member EnsureInitialized () = checkInitialized()

    /// Checks whether process configuration is initialized
    static member IsInitialized = !isInitialized

    /// Default FsPicklerSerializer instance.
    static member BinarySerializer = checkInitialized() ; VagabondRegistry.Instance.Serializer

    /// Default FsPicklerJsonSerializer instance.
    static member JsonSerializer = checkInitialized() ; jsonSerializer

    /// Working Directory used by current global state.
    static member WorkingDirectory = checkInitialized(); workingDirectory

    /// In-Memory cache
    static member ObjectCache = checkInitialized(); objectCache

    /// Local file system store
    static member FileStore = checkInitialized(); localFileStore

    /// MBrace.Azure compiled version
    static member Version = checkInitialized(); version

    /// Initializes process state for use as client.
    static member InitAsClient() = initGlobalState None true true
    /// Initializes process state for use as parent AppDomain in a worker.
    static member InitAsWorker(?workingDirectory : string) = initGlobalState workingDirectory true false
    /// Initializes process state for use as a slave AppDomain in a worker.
    static member InitAsWorkerSlaveDomain(workingDirectory : string) = initGlobalState (Some workingDirectory) false false