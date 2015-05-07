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


let getRandomName () =
    // See http://blogs.msdn.com/b/jmstall/archive/2014/06/12/azure-storage-naming-rules.aspx
    let alpha = [|'a'..'z'|]
    let alphaNumeric = Array.append alpha [|'0'..'9'|]
    let maxLen = 63
    let randOf =
        let rand = new Random(int DateTime.Now.Ticks)
        fun (x : char []) -> x.[rand.Next(0, x.Length)]

    let name = 
        [| yield randOf alpha
           for _i = 1 to maxLen-1 do yield randOf alphaNumeric |]
    new String(name)

open Microsoft.WindowsAzure.Storage.Table

let acc = CloudStorageAccount.Parse(store)
let client = acc.CreateCloudTableClient()
let name = getRandomName()
let table = client.GetTableReference(name)
table.CreateIfNotExists()


(table.CreateIfNotExistsAsync()).ContinueWith(fun _ -> ())
|> Async.AwaitTask
|> Async.RunSynchronously




let p = new System.Net.NetworkInformation.Ping()
p.Send("google.com")










let ns = NamespaceManager.CreateFromConnectionString(sbus)
let qd = new QueueDescription("tmp")
qd.LockDuration <- TimeSpan.FromMinutes(2.)
ns.CreateQueue(qd) |> ignore
let queue = QueueClient.CreateFromConnectionString(sbus, "tmp")


let m = new BrokeredMessage("Hello")
m.Properties.Add("foo", "bar")
m.Properties.Add("goo", "bar")
m.Size
m.
queue.Send(m) //(200 - 68)

queue.SendBatch([m]) //(200 - 68)

let ms = Array.init 1500 (fun i -> new BrokeredMessage(sprintf "Hello %d" i))
ms |> Seq.sumBy (fun m -> m.Size)

queue.SendBatch(ms)
queue.PrefetchCount
qd.MessageCount

let ms' = queue.ReceiveBatch(1000)
ms' |> Seq.iter (fun msg -> printfn "%s" <| msg.GetBody<string>(); msg.Complete())


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


let b = container.GetBlockBlobReference("foo.txt")


b.Properties
b.DownloadToStream()
let s = b.OpenWrite()
s.Write([|1uy..10uy|],0, 10)
s.Dispose()

b.UploadText("Hello world")



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

