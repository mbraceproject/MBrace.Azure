#I "../../bin/"
#r "Vagrant.dll"
#r "MBrace.Core.dll"
#r "MBrace.Library.dll"
#r "MBrace.Azure.Runtime.Common.dll"
#r "MBrace.Azure.Runtime.dll"
#r "MBrace.Azure.Client.dll"

open Nessos.MBrace
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Client


// Create your azure specific configuration.
let config = 
    { Configuration.Default with
        StorageConnectionString    = "[connection string]"
        ServiceBusConnectionString = "[connection string]"
    }

// Wait until at least one worker is active, 
// get a handle for this runtime.
let client = Runtime.GetHandle(config, waitWorkerCount = 1)

client.ShowWorkers()




let sayHello = cloud { return "Hello world" }




let ps = client.CreateProcess(sayHello)

client.ShowProcess(ps.Id)

ps.AwaitResult()
