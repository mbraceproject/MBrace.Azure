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


type ClusterSession(config : MBrace.Azure.Configuration, workerCount : int) =

    static do AzureWorker.LocalExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.azureworker.exe"
    
    let lockObj = obj ()
    let mutable state = None

    member __.Start () =
        lock lockObj (fun () ->
            match state with
            | Some _ -> invalidOp "MBrace runtime already initialized."
            | None -> 
                let runtime = 
                    if workerCount < 1 then
                        AzureCluster.Connect(config, logger = ConsoleLogger(), logLevel = LogLevel.Debug)
                    else
                        AzureCluster.InitOnCurrentMachine(config, workerCount, maxWorkItems = 32, logger = ConsoleLogger(), logLevel = LogLevel.Debug)

                while runtime.Workers.Length < workerCount do Thread.Sleep 100
                state <- Some runtime)

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
            while cluster.Workers.Length <> 0 do 
                Thread.Sleep 1000
                cluster.CullNonResponsiveWorkers(TimeSpan.FromSeconds 5.)

            cluster.AttachLocalWorkers(workerCount)
            while cluster.Workers.Length <> workerCount do Thread.Sleep 500)