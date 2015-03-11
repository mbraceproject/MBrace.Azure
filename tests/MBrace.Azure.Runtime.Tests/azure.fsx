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
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Blob
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
ns.CreateQueue(qd) |> ignore
let queue = QueueClient.CreateFromConnectionString(sbus, "tmp")

queue.Send(new BrokeredMessage("Hello world"))

let bm = queue.Receive()

bm.RenewLock()


queue.Complete(bm.LockToken)


//
//type AutoRenewLease (blob : ICloudBlob) =
//    // Acquire infinite lease
//    let proposed = System.Guid.NewGuid() |> string
//    let leaseId = blob.AcquireLease(Unchecked.defaultof<_> , proposed)
//    
//    interface IDisposable with
//        member this.Dispose(): unit = blob.ReleaseLease(AccessCondition.GenerateLeaseCondition(leaseId))
//            
//    member this.HasLock = true
//    member this.ProposedId = proposed
//    member this.LeaseId = leaseId


let acc = CloudStorageAccount.Parse(store)
let client = acc.CreateCloudBlobClient()
let container = client.GetContainerReference("temp")
container.CreateIfNotExists()



[ 1..10 ]
|> List.map (fun i -> container.GetBlockBlobReference("foo.txt"))
|> List.mapi (fun b i -> async { return i.UploadText(String.init (1024 * 10) (fun _ ->string b)) } )
|> Async.Parallel
|> Async.Ignore
|> Async.RunSynchronously

blob1.DownloadText()
blob2.DownloadText()
blob1.Delete()
let ac1 = AccessCondition.GenerateEmptyCondition()

blob1.UploadText()
let l1 = new AutoRenewLease(blob1)

