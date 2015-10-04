namespace MBrace.Azure.Runtime

open Nessos.FsPickler

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Components
open MBrace.Azure.Runtime.Utilities

/// Provides functionality for persisting values/closures to blob store and performs object sifting.
[<AbstractClass; Sealed; AutoSerializable(false)>]
type BlobPersist private () =

    static let siftClosures = true // TODO: move to configuration

    static let mkBlobValue (id : ClusterId) (fileName : string) =
        BlobValue.Define<'T>(id.StorageAccount, id.RuntimeContainer, fileName)
    
    /// <summary>
    ///     Persists object to blob store with supplied configuration parameters.
    /// </summary>
    /// <param name="id">Cluster identifier object.</param>
    /// <param name="closure">Closure value to be serialized.</param>
    /// <param name="persistFileName">Filename of persisted blob.</param>
    /// <param name="allowNewSifts">Specifies if new values should be sifted.</param>
    static member PersistClosure<'T>(id : ClusterId, closure : 'T, persistFileName : string, allowNewSifts : bool) = async {
        if siftClosures then
            let manager = ConfigurationRegistry.Resolve<ClosureSiftManager>(id)
            let! sifted = manager.SiftClosure(closure, allowNewSifts)
            let bv : BlobValue<SiftedClosure<'T>> = mkBlobValue id persistFileName
            do! bv.WriteValue sifted
        else
            let bv : BlobValue<'T> = mkBlobValue id persistFileName
            do! bv.WriteValue closure
    }

    /// <summary>
    ///     Reads persisted value of given type form blob store with supplied configuration.
    /// </summary>
    /// <param name="id">Cluster identifier object.</param>
    /// <param name="persistFileName">Filename of persisted blob.</param>
    static member ReadPersistedClosure<'T>(id : ClusterId, persistFileName : string) = async {
        if siftClosures then
            let manager = ConfigurationRegistry.Resolve<ClosureSiftManager>(id)
            let bv : BlobValue<SiftedClosure<'T>> = mkBlobValue id persistFileName
            let! sifted = bv.GetValue()
            return! manager.UnSiftClosure(sifted)
        else
            let bv : BlobValue<'T> = mkBlobValue id persistFileName
            return! bv.GetValue()
    }

    /// Deletes persisted blob of given name.
    static member DeletePersistedClosure(id : ClusterId, persistedFileName : string) = async {
        let bv : BlobValue<obj> = mkBlobValue id persistedFileName
        do! bv.Delete()
    }

    /// Gets size of persisted closure in bytes.
    static member GetPersistedClosureSize(id : ClusterId, persistedFileName : string) = async {
        let bv : BlobValue<obj> = mkBlobValue id persistedFileName
        return! bv.GetSize()
    }