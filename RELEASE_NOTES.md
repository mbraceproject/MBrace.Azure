### 1.5.2
* Upgrade FsPickler version.

### 1.5.1
* Fix Azure.Management packaging issue.

### 1.5.0
* Update to FSharp.Core 4.4.1.0 (F# 4.1/VS2017 fsi.exe)

### 1.4.3
* Fix nuget package issue.

### 1.4.2
* Fix nuget package issue.

### 1.4.1
* Upgrade to latest MBrace.Core.

### 1.4.0
* Upgrade to latest MBrace.Core.
* Add support for Azure environment loading of Argu parameters in standalone workers.

### 1.3.0
* Upgrade to latest MBrace.Core.

### 1.2.3
* Update package dependencies.
* Misc bugfixes and improvements.

### 1.2.2
* Update package dependencies.

### 1.2.1
* Fix packaging issue.

### 1.2
* Move to MBrace.Core 1.1

### 1.1.25
* Improve provisioning reliability
* Better provisioning status reporting

### 1.1.24
* Fix packaging issue.

### 1.1.23
* minimize amount of calls to ServiceBus subscription management API.

### 1.1.22
* minimize amount of calls to ServiceBus subscription management API.

### 1.1.21
* Update MBrace.Core version.

### 1.1.20
* Bugfixes.

### 1.1.19
* Fix packaging issues.

### 1.1.18
* Improvements for unix environments.

### 1.1.17
* Update MBrace.Core.

### 1.1.16
* Update MBrace.Core.

### 1.1.15
* Fix Vagabond issue.

### 1.1.14
* Update MBrace.Core.

### 1.1.13
* Fix packaging issue.

### 1.1.12
* Fix deployment package names in blob storage.

### 1.1.11
* Update MBrace.Core version.

### 1.1.10
* Add support for automatic deletion of storage and service bus accounts on cluster deprovision.
* Expose storage and service bus account info for deployment instances.

### 1.1.9
* Expose cluster label information to deployment info.

### 1.1.8
* Make support for storage/servicebus account reuse optional in Management lib.

### 1.1.7
* Add support for unicode file names in blob store.

### 1.1.6
* Enable ServerGC by default in Worker configurations.

### 1.1.5
* Enable elevated permissions in default cloud service definition.

### 1.1.4
* Add locality checks for storage and service bus accounts.

### 1.1.3
* Minor fixes.

### 1.1.2
* Fix packaging issues.

### 1.1.1
* Fix packaging issue.

### 1.1.0
* Refactorings and improvements to MBrace.Azure.Management.
* Misc bugfixes.

### 1.0.0
* 1.0 Release

### 0.16.3-beta
* Fix packaging issues.

### 0.16.2-beta
* Provisioning API changes and improvements.

### 0.16.1-beta
* Add provisioning API

### 0.16.0-beta
* Update to latest MBrace.Core

### 0.15.2-beta
* Fix CloudFlow performance issues.

### 0.15.1-beta
* Fix client performance issues.

### 0.15.0-beta
* Update to support MBrace.Core 0.15

### 0.14.1-beta
* Update to support MBrace.Core 0.14.1

### 0.14.0-beta
* Update to support MBrace.Core 0.14

### 0.13.1-beta
* Minor improvements and bugfixes.

### 0.13.0-beta
* Update to support the latest MBrace.Core release.

### 0.12.3-beta
* Add support for forced local FSharp.Core binding redirects.

### 0.12.2-beta
* Fix packaging issue.

### 0.12.1-beta
* Fix packaging issue.

### 0.12.0-beta
* Refactor MBrace.Azure internals.
* Improve MBrace.Azure public APIs.
* Multiple bugfixes and improvements.

### 0.10.6-alpha
* Fix packaging issue.

### 0.10.5-alpha
* Upgrade MBrace.Core to 0.11.
* Refactor and improve logging.

### 0.10.4-alpha
* Fix packaging issue.

### 0.10.3-alpha
* Upgrade to lastest MBrace.Core.
* Fix Vagabond initialization issue in worker roles.
* Implement worker-based logging.

### 0.10.2-alpha
* Improve support for multiple client sessions in single process.

### 0.10.1-alpha
* Fix issue #107.

### 0.10.0-alpha
* Migrate to MBrace.Core 0.10 APIs
* Fix MBraceAzure.ClearAllCloudTasks bug.
* Client and Service API changed to match MBrace.Core.
* Added extra options when spawning local workers.

### 0.7.0-alpha
* Migrate to MBrace.Runtime.Core 0.9.14 APIs
* Client API changes.
* Configuration API changes.
* Service API changed.
* Misc bugfixes.

### 0.6.10-alpha
* Update MBrace.Core libraries.
* Process.Kill renamed to Cancel for clarity.
* Fixed active workItem count bug for long running jobs.
* Fixed an issue when serializing Configuration in queue metadata.

### 0.6.9-alpha
* Fix location of temporary files used by the runtime.
* Add KillLocalWorkers.
* Table based CloudDictionary no longer supports Count/Size properties.

### 0.6.8-alpha
* Fix NuGet dependencies.

#### 0.6.7-alpha
* Update MBrace.Core libraries.
* Misc bugfixes.

#### 0.6.6-alpha
* Update MBrace.Core libraries.
* Improved status reporting for workers and bugfixes.

#### 0.6.5-alpha
* Update MBrace.Core libraries.
* Remove transient dependencies from nuget package.
* Use improved Vagabond implementation.
* Isolate runtime folders by version.
* Misc bugfixes.

#### 0.6.4-alpha
* Update MBrace.Core libraries.
* Implement AppDomain isolation for user jobs.
* Add Standalone runtime support.
* Misc bugfixes and improvements.

#### 0.6.3-alpha
* Hotfix packaging issue.

#### 0.6.2-alpha
* Hotfix packaging issue.

#### 0.6.1-alpha
* Introduce Local workflows.
* Refactoring of store folders used by runtime.
* Misc bugfixes and improvements.

#### 0.6.0-alpha
* Initial nuget release.