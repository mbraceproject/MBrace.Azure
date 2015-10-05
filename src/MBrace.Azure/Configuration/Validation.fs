namespace MBrace.Azure.Runtime

open System.Text.RegularExpressions

[<RequireQualifiedAccess>]
module Validate =

    // http://blogs.msdn.com/b/jmstall/archive/2014/06/12/azure-storage-naming-rules.aspx
    // https://msdn.microsoft.com/en-us/library/azure/dd135715.aspx

    let private lowerAlphaNumeric = Regex("^[a-z0-9]*$", RegexOptions.Compiled)
    let private lowerAlphaNumericDash = Regex("^[a-z0-9\-]*$", RegexOptions.Compiled)
    let private alphaNumeric = Regex("^[a-zA-Z0-9]*$", RegexOptions.Compiled)
    let private alphaNumericDashes = Regex("^[a-zA-Z0-9\-_/]*$", RegexOptions.Compiled)

    let inline private validate (r : Regex) (input : string) = r.IsMatch input

    let tableName (tableName : string) =
        let exn () = invalidArg "tableName" (sprintf "Invalid table name %A. Name length should be between 3 and 63, containing alphanumeric characters." tableName)
        if tableName.Length < 3 || tableName.Length > 63 then exn ()
        if not <| validate alphaNumeric tableName then exn ()

    let containerName (containerName : string) = 
        let exn () = invalidArg "containerName" (sprintf "Invalid container name %A. Name length should be between 3 and 63, containing lowercase characters, numbers or '-' delimiters." containerName)
        if containerName = "" || containerName = "$root" then ()
        elif containerName.Length < 3 || containerName.Length > 63 then exn ()
        elif not <| validate lowerAlphaNumericDash containerName then exn ()
        elif containerName.[0] = '-' then exn ()

    let queueName (queueName : string) = 
        let exn () = invalidArg "queueName" (sprintf "Invalid queue name %A. Name should contain only alphanumeric characters or '-','_','/' delimiters." queueName)
        if not <| validate alphaNumericDashes queueName then exn ()
                
    let subscriptionName (subscriptionName : string) = 
        let exn () = invalidArg "subscriptionName" (sprintf "Invalid subscription name %A. Name length should be less than 50, containing only alphanumeric characters or '-','_','/' delimiters." subscriptionName)
        if subscriptionName.Length > 50 then exn ()
        if not <| validate alphaNumericDashes subscriptionName then exn ()