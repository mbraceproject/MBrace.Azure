module internal MBrace.Azure.StandaloneWorker

[<EntryPoint>]
let main (args : string []) =
    System.Configuration.Azure.applyAzureEnvironmentToConfigurationManager ()
    MBrace.Azure.Service.StandaloneWorker.main args