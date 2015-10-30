#I __SOURCE_DIRECTORY__
#r @"tools\Newtonsoft.Json.dll"
#r @"tools\FsPickler.Json.dll"
#r @"tools\FsPickler.dll"
#r @"tools\System.Spatial.dll"
#r @"tools\Mono.Cecil.dll"
#r @"tools\Vagabond.AssemblyParser.dll"
#r @"tools\Vagabond.dll"
#r @"tools\Argu.dll"
#r @"tools\Microsoft.Data.Edm.dll"
#r @"tools\Microsoft.Data.OData.dll"
#r @"tools\Microsoft.Data.Services.Client.dll"
#r @"tools\Microsoft.ServiceBus.dll"
#r @"tools\Microsoft.WindowsAzure.Configuration.dll"
#r @"tools\Microsoft.WindowsAzure.Storage.dll"
#r @"tools\MBrace.Core.dll"
#r @"tools\MBrace.Runtime.dll"
#r @"tools\MBrace.Azure.dll"

// Needed for managment operations like provisioning:
#r @"tools\Hyak.Common.dll"
#r @"tools\Microsoft.Azure.Common.dll"
#r @"tools\Microsoft.Threading.Tasks.dll"
#r @"tools\Microsoft.Azure.Common.NetFramework.dll"
#r @"tools\Microsoft.WindowsAzure.Management.dll"
#r @"tools\Microsoft.WindowsAzure.Management.Compute.dll"
#r @"tools\Microsoft.WindowsAzure.Management.Storage.dll"
#r @"tools\Microsoft.WindowsAzure.Management.ServiceBus.dll"
#r "System.Runtime"
#r "System.Threading.Tasks"
open System.IO
open MBrace.Azure

AzureWorker.LocalExecutable <- Path.Combine(__SOURCE_DIRECTORY__, @"tools\mbrace.azureworker.exe")