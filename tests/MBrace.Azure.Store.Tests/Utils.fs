module internal MBrace.Azure.Store.Tests.Utils 

open System

let selectEnv name =
    (Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.User),
        Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.Machine),
        Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.Process))
    |> function 
        | s, _, _ when not <| String.IsNullOrEmpty(s) -> s
        | _, s, _ when not <| String.IsNullOrEmpty(s) -> s
        | _, _, s when not <| String.IsNullOrEmpty(s) -> s
        | _ -> failwith "Variable not found"