### 0.10.0-alpha
* Migrate to MBrace.Core 0.10 APIs
* Fix MBraceAzure.ClearAllCloudTasks bug.

### 0.7.0-alpha
* Migrate to MBrace.Runtime.Core 0.9.14 APIs
* Client API changes.
* Configuration API changes.
* Service API changed.
* Misc bugfixes.

### 0.6.10-alpha
* Update MBrace.Core libraries.
* Process.Kill renamed to Cancel for clarity.
* Fixed active job count bug for long running jobs.
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