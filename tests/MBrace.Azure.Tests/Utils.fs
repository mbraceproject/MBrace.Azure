namespace MBrace.Azure.Tests

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


type RuntimeSession(config : MBrace.Azure.Configuration, localWorkers : int) =

    static do AzureCluster.LocalWorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.azureworker.exe"
    
    let mutable state = None

    member __.Start () =
        match state with
        | Some _ -> invalidOp "MBrace runtime already initialized."
        | None -> 
            let runtime = 
                if localWorkers < 1 then
                    AzureCluster.Connect(config, logger = ConsoleLogger(), logLevel = LogLevel.Debug)
                else
                    AzureCluster.InitOnCurrentMachine(config, localWorkers, maxWorkItems = 32, logger = ConsoleLogger(), logLevel = LogLevel.Debug)
            state <- Some runtime

    member __.Stop () =
        state |> Option.iter (fun r -> (r.KillAllLocalWorkers() ; r.Reset(deleteUserData = true, deleteAssemblyData = true, force = true, reactivate = false)))
        state <- None

    member __.Runtime =
        match state with
        | None -> invalidOp "MBrace runtime not initialized."
        | Some r -> r