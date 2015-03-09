#I "../../bin/"
#r "FsPickler.dll"
#r "Vagabond.dll"
#r "Microsoft.ServiceBus.dll"
#r "Microsoft.WindowsAzure.Storage.dll"
#r "System.ServiceModel"
#r "System.Runtime.Serialization"
#time "on"


// Playground - using directly azure primitives
open System
open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging

let selectEnv name =
    (Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.User),
        Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.Machine))
    |> function | null, s | s, null | s, _ -> s


let store  = selectEnv "azurestorageconn"
let sbus = selectEnv "azureservicebusconn" 


let ns = NamespaceManager.CreateFromConnectionString(sbus)
let qd = new QueueDescription("tmp") in qd.LockDuration <- TimeSpan.FromMinutes(2.)
ns.CreateQueue(qd)
let queue = QueueClient.CreateFromConnectionString(sbus, "tmp")

queue.Send(new BrokeredMessage("Hello world"))

let bm = queue.Receive()

bm.RenewLock()


queue.Complete(bm.LockToken)

