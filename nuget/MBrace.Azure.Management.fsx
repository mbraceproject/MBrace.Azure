#I __SOURCE_DIRECTORY__
#I "../Hyak.Common/lib/net40/"
#I "../Microsoft.Bcl.Async/lib/net40"
#I "../Microsoft.Azure.Common/lib/net40/"
#I "../Microsoft.WindowsAzure.Management/lib/net40/"
#I "../Microsoft.WindowsAzure.Management.Compute/lib/net40/"
#I "../Microsoft.WindowsAzure.Management.Storage/lib/net40/"
#I "../Microsoft.WindowsAzure.Management.ServiceBus/lib/net40/"
#I "../MBrace.Core/lib/net45/"
#I "../MBrace.Runtime/lib/net45/"
#I "../MBrace.Azure/lib/net45/"
#I "lib/net45/"

#r "System.Runtime"
#r "System.Threading.Tasks"
#r "Hyak.Common.dll"
#r "Microsoft.Threading.Tasks.dll"
#r "Microsoft.Azure.Common.dll"
#r "Microsoft.Azure.Common.NetFramework.dll"
#r "Microsoft.WindowsAzure.Management.dll"
#r "Microsoft.WindowsAzure.Management.Compute.dll"
#r "Microsoft.WindowsAzure.Management.Storage.dll"
#r "Microsoft.WindowsAzure.Management.ServiceBus.dll"
#r "MBrace.Azure.Management.dll"
#r "MBrace.Runtime.dll"

open MBrace.Azure.Management

Config.DefaultLogger <- new ConsoleLogger()