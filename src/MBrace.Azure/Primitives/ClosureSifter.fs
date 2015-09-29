namespace MBrace.Azure.Runtime

open Nessos.FsPickler

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Components

[<AutoSerializable(false)>]
type ClosureSifter =
    
    static member SiftClosure<'T>(id : ClusterState, closure : 'T, allowNewSifts : bool) = async {
        let manager = ConfigurationRegistry.Resolve<ClosureSiftManager>(id)
        return! manager.SiftClosure(closure, allowNewSifts)
    }

    static member UnSiftClosure<'T>(id : ClusterState, sifted : SiftedClosure<'T>) = async {
        let manager = ConfigurationRegistry.Resolve<ClosureSiftManager>(id)
        return! manager.UnSiftClosure sifted
    }