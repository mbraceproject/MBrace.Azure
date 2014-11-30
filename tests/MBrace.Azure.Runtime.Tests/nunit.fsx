#I "../../bin/"
#r "Vagrant.dll"
#r "MBrace.Core.dll"
#r "MBrace.Library.dll"
#r "MBrace.Azure.Runtime.Common.dll"
#r "MBrace.Azure.Runtime.dll"
#r "MBrace.Azure.Client.dll"
#r "MBrace.Azure.Runtime.Tests.dll"
#r "Nunit.Framework.dll"
#I @"..\..\packages\NUnit.Runners.2.6.3\tools\lib\"
#r "Nunit.Core"
#r "Nunit.Core.Interfaces"

open System
open System.IO
open Nessos.MBrace
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Client
open NUnit.Core


let testPackage = new TestPackage(Path.Combine(__SOURCE_DIRECTORY__ + @"../../../bin/MBrace.Azure.Runtime.Tests.dll"))
let remoteTestRunner = new RemoteTestRunner()
remoteTestRunner.Load(testPackage)
let testResult = remoteTestRunner.Run(QueuingEventListener(), TestFilter.Empty, true, LoggingThreshold.All)

let tests = Nessos.MBrace.Azure.Runtime.Tests.``Azure Runtime Tests``()
tests.Init()

tests.``1. Parallel : empty input``()

tests.``1. Parallel : exception cancellation``()

