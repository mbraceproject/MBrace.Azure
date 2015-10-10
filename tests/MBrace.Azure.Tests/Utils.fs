namespace MBrace.Azure.Tests

open System
open System.Threading

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Azure.Runtime
open MBrace.Azure

#nowarn "445"

open MBrace.Core.Tests

[<AutoOpenAttribute>]
module Utils =

    let mkRemoteConfig () = Configuration.FromEnvironmentVariables()
    let mkEmulatorConfig () = new Configuration("UseDevelopmentStorage=true", Configuration.EnvironmentServiceBusConnectionString)


type ClusterSession(config : MBrace.Azure.Configuration, localWorkerCount : int, ?heartbeatThreshold : TimeSpan) =

    static do AzureWorker.LocalExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.azureworker.exe"
    
    let heartbeatThreshold = defaultArg heartbeatThreshold (TimeSpan.FromSeconds 10.)
    let lockObj = obj ()
    let mutable state = None

    let attachWorkers (cluster : AzureCluster) =
        cluster.AttachLocalWorkers(workerCount = localWorkerCount, logLevel = LogLevel.Debug, heartbeatThreshold = heartbeatThreshold)

    member __.Start () =
        lock lockObj (fun () ->
            match state with
            | Some _ -> invalidOp "MBrace runtime already initialized."
            | None -> 
                let cluster = AzureCluster.Connect(config, logger = ConsoleLogger(), logLevel = LogLevel.Debug)
                if localWorkerCount > 0 then 
                    cluster.Reset(force = false, deleteUserData = true, deleteAssemblyData = true, reactivate = true)
                    attachWorkers cluster

                while cluster.Workers.Length < localWorkerCount do Thread.Sleep 100
                state <- Some cluster)

    member __.Stop () =
        lock lockObj (fun () ->
            match state with
            | None -> ()
            | Some r -> 
                if localWorkerCount > 0 then 
                    r.KillAllLocalWorkers()
                    r.Reset(deleteUserData = true, deleteAssemblyData = true, force = true, reactivate = false)

                state <- None)

    member __.Cluster =
        match state with
        | None -> invalidOp "MBrace runtime not initialized."
        | Some r -> r

    member __.Chaos() =
        if localWorkerCount < 1 then () else
        lock lockObj (fun () ->
            let cluster = __.Cluster
            cluster.KillAllLocalWorkers()
            while cluster.Workers.Length > 0 do Thread.Sleep 500
            attachWorkers cluster
            while cluster.Workers.Length < localWorkerCount do Thread.Sleep 500)