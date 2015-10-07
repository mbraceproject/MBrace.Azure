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
    open System

    let private selectEnv name =
        (Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.User),
          Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.Machine),
            Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.Process))
        |> function 
           | s, _, _ when not <| String.IsNullOrEmpty(s) -> s
           | _, s, _ when not <| String.IsNullOrEmpty(s) -> s
           | _, _, s when not <| String.IsNullOrEmpty(s) -> s
           | _ -> failwithf "Variable %A not found" name

    let remoteConfig = new Configuration(selectEnv "azurestorageconn", selectEnv "azureservicebusconn")
    let emulatorConfig = new Configuration("UseDevelopmentStorage=true", selectEnv "azureservicebusconn")


type LocalClusterSession(config : MBrace.Azure.Configuration, workerCount : int, ?heartbeatThreshold : TimeSpan) =

    static do AzureWorker.LocalExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.azureworker.exe"
    
    let heartbeatThreshold = defaultArg heartbeatThreshold (TimeSpan.FromSeconds 10.)
    let lockObj = obj ()
    let mutable state = None

    let attachWorkers (cluster : AzureCluster) =
        if workerCount > 0 then
            cluster.AttachLocalWorkers(workerCount = workerCount, logLevel = LogLevel.Debug, heartbeatThreshold = heartbeatThreshold)

    member __.Start () =
        lock lockObj (fun () ->
            match state with
            | Some _ -> invalidOp "MBrace runtime already initialized."
            | None -> 
                let cluster = AzureCluster.Connect(config, logger = ConsoleLogger(), logLevel = LogLevel.Debug)
                cluster.Reset(force = false, deleteUserData = true, deleteAssemblyData = true, reactivate = true)
                do attachWorkers cluster
                while cluster.Workers.Length < workerCount do Thread.Sleep 100
                state <- Some cluster)

    member __.Stop () =
        lock lockObj (fun () ->
            match state with
            | None -> ()
            | Some r -> 
                r.KillAllLocalWorkers() 
                r.Reset(deleteUserData = true, deleteAssemblyData = true, force = true, reactivate = false)
                state <- None)

    member __.Cluster =
        match state with
        | None -> invalidOp "MBrace runtime not initialized."
        | Some r -> r

    member __.Chaos() =
        lock lockObj (fun () ->
            let cluster = __.Cluster
            cluster.KillAllLocalWorkers()
            while cluster.Workers.Length > 0 do Thread.Sleep 500
            attachWorkers cluster
            while cluster.Workers.Length < workerCount do Thread.Sleep 500)