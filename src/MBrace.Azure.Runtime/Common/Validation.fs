namespace MBrace.Azure.Runtime

[<RequireQualifiedAccess>]
module Validate =
    open Microsoft.WindowsAzure.Storage
    open Microsoft.ServiceBus

    // http://blogs.msdn.com/b/jmstall/archive/2014/06/12/azure-storage-naming-rules.aspx

    let private lower = set { 'a'..'z' }
    let private upper = set { 'A'..'Z' }
    let private alpha = lower + upper
    let private number = set { '0'..'9' }
    let private alphanumeric = alpha + number

    let storageConn conn =
        match CloudStorageAccount.TryParse(conn) with
        | true, _ -> ()
        | false, _ -> raise(InvalidConfigurationException(sprintf "Invalid Storage connection string %A" conn))
        conn

    let serviceBusConn conn =
        try
            NamespaceManager.CreateFromConnectionString(conn).QueueExists("foobar")
            |> ignore
        with ex -> raise(InvalidConfigurationException(sprintf "Invalid ServiceBus connection string %A" conn, ex))
        conn

    let tableName (name : string) =
        let exn = InvalidConfigurationException(sprintf "Invalid table name %A. See Azure Table Storage naming rules." name)
        if name.Length < 3 || name.Length > 63 then raise exn
        if name |> Seq.forall alphanumeric.Contains |> not then raise exn
        name

    let containerName (name : string) = 
        let exn = InvalidConfigurationException(sprintf "Invalid container name %A. See Azure Blob Storage naming rules." name)
        if name.Length < 3 || name.Length > 63 then raise exn
        if name |> Seq.forall (lower + number).Contains |> not then raise exn
        name

    let queueName (name : string) = 
        let exn = InvalidConfigurationException(sprintf "Invalid queue name %A. See Azure Service Bus naming rules." name)
        if name |> Seq.forall (alphanumeric + set ['-'; '_'; '/']).Contains |> not then raise exn
        name
                
    let subscriptionName (name : string) = 
        let exn = InvalidConfigurationException(sprintf "Invalid subscription name %A. See Azure Service Bus naming rules." name)
        if name.Length > 50 then raise exn
        if name |> Seq.forall (alphanumeric + set ['-'; '_';]).Contains |> not then raise exn
        name