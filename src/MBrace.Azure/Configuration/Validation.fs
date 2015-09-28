namespace MBrace.Azure.Runtime

open System.Text.RegularExpressions

[<RequireQualifiedAccess>]
module Validate =

    // http://blogs.msdn.com/b/jmstall/archive/2014/06/12/azure-storage-naming-rules.aspx

    let private lowerAlphaNumeric = Regex("^[a-z0-9]*$", RegexOptions.Compiled)
    let private alphaNumeric = Regex("^[a-zA-Z0-9]*$", RegexOptions.Compiled)
    let private alphaNumericHyphens = Regex("^[a-zA-Z0-9\-_/]*$", RegexOptions.Compiled)

    let inline private validate (r : Regex) (input : string) = r.IsMatch input

    let tableName (name : string) =
        let exn = InvalidConfigurationException(sprintf "Invalid table name %A. Name length should be between 3 and 63, containing alphanumeric characters." name)
        if name.Length < 3 || name.Length > 63 then raise exn
        if not <| validate alphaNumeric name then raise exn

    let containerName (name : string) = 
        let exn = InvalidConfigurationException(sprintf "Invalid container name %A. Name length should be between 3 and 63, containing lowercase characters or numbers." name)
        if name.Length < 3 || name.Length > 63 then raise exn
        if not <| validate lowerAlphaNumeric name then raise exn

    let queueName (name : string) = 
        let exn = InvalidConfigurationException(sprintf "Invalid queue name %A. Name should contain only alphanumeric characters or '-','_','/' delimiters." name)
        if not <| validate alphaNumericHyphens name then raise exn
                
    let subscriptionName (name : string) = 
        let exn = InvalidConfigurationException(sprintf "Invalid subscription name %A. Name length should be less than 50, containing only alphanumeric characters or '-','_','/' delimiters." name)
        if name.Length > 50 then raise exn
        if not <| validate alphaNumericHyphens name then raise exn