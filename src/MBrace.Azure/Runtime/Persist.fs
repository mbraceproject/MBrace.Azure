namespace MBrace.Azure.Runtime

open MBrace.FsPickler

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Components
open MBrace.Azure.Runtime.Utilities

/// Provides functionality for persisting values/closures to blob store and performs object sifting.
[<AbstractClass; Sealed; AutoSerializable(false)>]
type BlobPersist private () =

    static let mkBlobValue (clusterId : ClusterId) (fileName : string) =
        BlobValue.Define<'T>(clusterId.StorageAccount, clusterId.RuntimeContainer, fileName)
    
    /// <summary>
    ///     Persists object to blob store with supplied configuration parameters.
    /// </summary>
    /// <param name="clusterId">Cluster identifier object.</param>
    /// <param name="closure">Closure value to be serialized.</param>
    /// <param name="persistFileName">Filename of persisted blob.</param>
    /// <param name="allowNewSifts">Specifies if new values should be sifted.</param>
    static member PersistClosure<'T>(clusterId : ClusterId, closure : 'T, persistFileName : string, allowNewSifts : bool) = async {
        if clusterId.OptimizeClosureSerialization then
            let manager = ConfigurationRegistry.Resolve<ClosureSiftManager>(clusterId)
            let! sifted = manager.SiftClosure(closure, allowNewSifts)
            let bv : BlobValue<SiftedClosure<'T>> = mkBlobValue clusterId persistFileName
            do! bv.WriteValue sifted
        else
            let bv : BlobValue<'T> = mkBlobValue clusterId persistFileName
            do! bv.WriteValue closure
    }

    /// <summary>
    ///     Reads persisted value of given type form blob store with supplied configuration.
    /// </summary>
    /// <param name="clusterId">Cluster identifier object.</param>
    /// <param name="persistFileName">Filename of persisted blob.</param>
    static member ReadPersistedClosure<'T>(clusterId : ClusterId, persistFileName : string) = async {
        if clusterId.OptimizeClosureSerialization then
            let manager = ConfigurationRegistry.Resolve<ClosureSiftManager>(clusterId)
            let bv : BlobValue<SiftedClosure<'T>> = mkBlobValue clusterId persistFileName
            let! sifted = bv.GetValue()
            return! manager.UnSiftClosure(sifted)
        else
            let bv : BlobValue<'T> = mkBlobValue clusterId persistFileName
            return! bv.GetValue()
    }

    /// Deletes persisted blob of given name.
    static member DeletePersistedClosure(clusterId : ClusterId, persistedFileName : string) = async {
        let bv : BlobValue<obj> = mkBlobValue clusterId persistedFileName
        do! bv.Delete()
    }

    /// Gets size of persisted closure in bytes.
    static member GetPersistedClosureSize(clusterId : ClusterId, persistedFileName : string) = async {
        let bv : BlobValue<obj> = mkBlobValue clusterId persistedFileName
        return! bv.GetSize()
    }