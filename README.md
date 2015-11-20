[![Join the chat at https://gitter.im/mbraceproject/MBrace.Azure](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/mbraceproject/MBrace.Azure?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![NuGet Status](http://img.shields.io/nuget/vpre/MBrace.Azure.svg?style=flat)](https://www.nuget.org/packages/MBrace.Azure/)

# MBrace on Azure

An MBrace runtime implementation on top of Azure PaaS components. 
Enables easy deployment of scalable MBrace clusters using worker roles. 
It also supports on-site cluster deployments using Azure storage/service bus components for communication.

For a first introduction to MBrace please refer to the main website at [m-brace.net](http://www.m-brace.net/).
For developer information regarding the MBrace Core components, please refer to the [MBrace.Core](https://github.com/mbraceproject/MBrace.Core) repository.
If you have any questions regarding MBrace don't hesitate to create an issue or ask one of the [maintainers](#maintainers).
You can also follow the official MBrace twitter account [@mbracethecloud](https://twitter.com/mbracethecloud).

## Development and Testing

### Building

Development of MBrace.Azure requires Visual Studio 2015 and Azure SDK 2.7 installed on your computer.
You can build the project either from Visual Studio or by running `build.cmd Build` if using cmd/powershell or `./build.sh Build` if using bash/sh.

Before attempting to test MBrace.Azure on your machine, please make sure that your local time setting is correct.
A significant time offset might create issues when attempting to deploy or communicate with an Azure cluster.

### Creating and Installing Azure Connection Strings

In order to use MBrace.Azure or run any of its unit tests, you must have access to an Azure account.
Follow the instructions below to install the required Azure connection strings in your local machine:
  1. [Create an Azure Storage account](https://azure.microsoft.com/en-in/documentation/articles/storage-create-storage-account/) and keep note of its connection string. Save the connection string in the `AzureStorageConnectionString` environment variable in your local computer.
  2. [Create an Azure Service Bus account](https://azure.microsoft.com/en-us/documentation/articles/service-bus-dotnet-how-to-use-queues/) and keep note of its connection string. Save the connection string in the `AzureServiceBusConnectionString` environment variable in your local computer.

### Provisioning an MBrace.Azure Service

Follow the instructions below to deploy an MBrace Service to Azure:
  1. Sign up for an [Azure account](https://azure.microsoft.com/en-us/) and [download your publication settings file](https://manage.windowsazure.com/publishsettings).
  2. From inside Visual Studio, navigate to `src/WorkerRole/MBrace.Azure.CloudService`.
  3. Expand the `Roles` folder and double click on `MBrace.Azure.WorkerRole`.
  4. In the `Configuration` tab, select your desired VM size.
  5. Close the Role menu and right click on the `MBrace.Azure.CloudService` icon on your solution, selecting the `Package..` option.
  6. Under `Build Configuration`, select either `Debug_AzureSDK` or `Release_AzureSDK` and hit `Package`.
  10. Go to [`samples/provision.fsx`](samples/provision.fsx) and follow the instructions there.

### Unit Tests

Unit tests can be run by calling `build.cmd RunTests` or `./build.sh RunTests` from the command line. 
Make sure that you have followed the connection string installation steps described above.

Unit tests can be run manually by opening the `MBrace.Azure.Tests.dll` assembly found in the repository's `/bin` folder using [NUnit-GUI](http://www.nunit.org/index.php?p=nunit-gui&r=2.2.10). 
Tests found here are separated in 4 categories:
  1. `Compute Emulator` run using the local Azure Compute Emulator and Storage Emulator, which need to be initialized manually.
  2. `Storage Emulator` run using local Azure worker processes and the Storage Emulator, which needs to be initialized manually.
  3. `Standalone Cluster` run using local Azure worker processes and an Azure-hosted storage account.
  4. `Remote Cluster` run using Azure-hosted worker instances, which need to be initialized manually.

## Contributing

The MBrace project is happy to accept quality contributions from the .NET community.
Please contact any of the maintainers if you would like to get involved.

## License

This project is subject to the [Apache Licence, Version 2.0](LICENSE.md).

## Maintainers

  * [@eiriktsarpalis](https://twitter.com/eiriktsarpalis)
  * [@krontogiannis](https://twitter.com/krontogiannis)

## Build Status

Head (branch master), Build & Vagabond tests
  * Windows/.NET [![Build status](https://ci.appveyor.com/api/projects/status/f0nt1f1ih0cwsa0o/branch/master?svg=true)](https://ci.appveyor.com/project/nessos/mbrace-azure/branch/master)
