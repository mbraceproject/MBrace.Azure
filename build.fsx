// --------------------------------------------------------------------------------------
// FAKE build script 
// --------------------------------------------------------------------------------------

#I "packages/FAKE/tools"
#r "packages/FAKE/tools/FakeLib.dll"

open Fake
open Fake.Git
open Fake.AssemblyInfoFile
open Fake.ReleaseNotesHelper

open System
open System.IO


let project = "MBrace.Azure"
let authors = [ "Nick Palladinos" ; "Kostas Rontogiannis" ; "Eirik Tsarpalis" ]

let description = """ MBrace on Windows Azure. """

let tags = "F# cloud mapreduce distributed azure windowsazure"

let azureSummary = """Contains the cluster/client implementation of MBrace on top of Azure PaaS."""
let standaloneSummary = """Standalone bundle of MBrace.Azure and dependencies for consumption by F# interactive and other scripting environments."""

// --------------------------------------------------------------------------------------
// Read release notes & version info from RELEASE_NOTES.md
Environment.CurrentDirectory <- __SOURCE_DIRECTORY__
let gitHash = Information.getCurrentHash()
let buildDate = DateTime.UtcNow
let release = parseReleaseNotes (IO.File.ReadAllLines "RELEASE_NOTES.md") 
let nugetVersion = release.NugetVersion

let gitHome = "https://github.com/mbraceproject"
let gitName = "MBrace.Azure"

let MBraceCoreVersion = "0.9.12-alpha"

// Generate assembly info files with the right version & up-to-date information
Target "AssemblyInfo" (fun _ ->
    let attributes =
        [| 
            Attribute.Title project
            Attribute.Product project
            Attribute.Company "Nessos Information Technologies"
            Attribute.Copyright "\169 Nessos Information Technologies."
            Attribute.Trademark "MBrace"
            Attribute.Metadata("Release Signature", 
                sprintf "Version %s, Git Hash %s, Build Date %s" 
                    release.AssemblyVersion
                    gitHash 
                    (buildDate.ToString "ddMMyyyy HH:mm zzz"))
            Attribute.Version release.AssemblyVersion
            Attribute.FileVersion release.AssemblyVersion
        |]

    !! "./src/**/AssemblyInfo.fs"
    |> Seq.iter (fun info -> CreateFSharpAssemblyInfo info attributes)
    !! "./samples/**/AssemblyInfo.fs"
    |> Seq.iter (fun info -> CreateFSharpAssemblyInfo info attributes)
    !! "./samples/**/AssemblyInfo.cs"
    |> Seq.iter (fun info -> CreateCSharpAssemblyInfo info attributes)
)


// --------------------------------------------------------------------------------------
// Clean and restore packages

Target "Clean" (fun _ ->
    CleanDirs (!! "**/bin/Release/")
    CleanDirs (!! "**/bin/Debug/")
    CleanDir "bin/"
)

// --------------------------------------------------------------------------------------
// Build


let configuration = environVarOrDefault "Configuration" "Release"

Target "Build" (fun _ ->
    // Build the rest of the project
    { BaseDirectory = __SOURCE_DIRECTORY__
      Includes = [ project + ".sln" ]
      Excludes = [] } 
    |> MSBuild "" "Build" ["Configuration", configuration]
    |> Log "AppBuild-Output: "
)

// --------------------------------------------------------------------------------------
// Run the unit tests using test runner & kill test runner when complete


let testAssemblies = 
    [
        "bin/MBrace.Azure.Runtime.Tests.dll"
        "bin/MBrace.Azure.Store.Tests.dll"
    ]

Target "RunTests" (fun _ ->
    let nunitVersion = GetPackageVersion "packages" "NUnit.Runners"
    let nunitPath = "packages/NUnit.Runners/tools" 
    ActivateFinalTarget "CloseTestRunner"

    testAssemblies
    |> NUnit (fun p -> 
        { p with
            DisableShadowCopy = false
            ToolPath = nunitPath
            Framework = "4.5"
            TimeOut = TimeSpan.FromMinutes 60. })
)

FinalTarget "CloseTestRunner" (fun _ ->  
    ProcessHelper.killProcess "nunit-agent.exe"
)

//// --------------------------------------------------------------------------------------
//// Build a NuGet package

let addFile (target : string) (file : string) =
    if File.Exists (Path.Combine("nuget", file)) then (file, Some target, None)
    else raise <| new FileNotFoundException(file)

let addAssembly reqXml (target : string) assembly =
    let includeFile force file =
        let file = file
        if File.Exists (Path.Combine("nuget", file)) then [(file, Some target, None)]
        elif force then raise <| new FileNotFoundException(file)
        else []

    seq {
        yield! includeFile true assembly
        yield! includeFile reqXml <| Path.ChangeExtension(assembly, "xml")
        yield! includeFile false <| Path.ChangeExtension(assembly, "pdb")
        yield! includeFile false <| assembly + ".config"
    }

Target "NuGet.Azure" (fun _ ->
    NuGet (fun p -> 
        { p with   
            Authors = authors
            Project = "MBrace.Azure"
            Summary = azureSummary
            Description = azureSummary
            Version = nugetVersion
            ReleaseNotes = String.concat " " release.Notes
            Tags = tags
            OutputPath = "bin"
            AccessKey = getBuildParamOrDefault "nugetkey" ""
            Dependencies = 
                [
                    "MBrace.Core", RequireExactly MBraceCoreVersion
                    "MBrace.Runtime.Core", RequireExactly MBraceCoreVersion
                    "WindowsAzure.Storage", "4.3.0"
                    "WindowsAzure.ServiceBus", "2.5.2"
                ]
            Publish = hasBuildParam "nugetkey" 
            Files =
                [
                    yield! addAssembly true @"lib\net45" @"..\bin\MBrace.Azure.Store.dll"
                    yield! addAssembly true @"lib\net45" @"..\bin\MBrace.Azure.Runtime.dll"
                    yield! addAssembly true @"lib\net45" @"..\bin\MBrace.Azure.Client.dll"
                ]
        })
        ("nuget/MBrace.Azure.nuspec")
)

Target "NuGet.Standalone" (fun _ ->
    NuGet (fun p -> 
        { p with   
            Authors = authors
            Project = "MBrace.Azure.Standalone"
            Summary = standaloneSummary
            Description = standaloneSummary
            Version = nugetVersion
            ReleaseNotes = String.concat " " release.Notes
            Tags = tags
            OutputPath = "bin"
            AccessKey = getBuildParamOrDefault "nugetkey" ""
            Publish = hasBuildParam "nugetkey" 
            Files =
                [
                    yield addFile @"" @"MBrace.Azure.fsx"
                    yield! addAssembly false @"tools" @"..\bin\Newtonsoft.Json.dll"
                    yield! addAssembly false @"tools" @"..\bin\FsPickler.dll"
                    yield! addAssembly false @"tools" @"..\bin\System.Spatial.dll"
                    yield! addAssembly false @"tools" @"..\bin\Mono.Cecil.dll"
                    yield! addAssembly false @"tools" @"..\bin\Vagabond.AssemblyParser.dll"
                    yield! addAssembly false @"tools" @"..\bin\Vagabond.dll"
                    yield! addAssembly false @"tools" @"..\bin\Microsoft.Data.Edm.dll"
                    yield! addAssembly false @"tools" @"..\bin\Microsoft.Data.OData.dll"
                    yield! addAssembly false @"tools" @"..\bin\Microsoft.Data.Services.Client.dll"
                    yield! addAssembly false @"tools" @"..\bin\Microsoft.ServiceBus.dll"
                    yield! addAssembly false @"tools" @"..\bin\Microsoft.WindowsAzure.Configuration.dll"
                    yield! addAssembly false @"tools" @"..\bin\Microsoft.WindowsAzure.Storage.dll"

                    yield! addAssembly true @"tools" @"..\bin\MBrace.Core.dll"
                    yield! addAssembly true @"tools" @"..\bin\MBrace.Runtime.Core.dll"
                    yield! addAssembly true @"tools" @"..\bin\MBrace.Azure.Store.dll"
                    yield! addAssembly true @"tools" @"..\bin\MBrace.Azure.Runtime.dll"
                    yield! addAssembly true @"tools" @"..\bin\MBrace.Azure.Client.dll"
                    yield! addAssembly false @"tools" @"..\bin\mbrace.azureworker.exe"
                ]
        })
        ("nuget/MBrace.Azure.nuspec")
)

// --------------------------------------------------------------------------------------
// documentation

Target "GenerateDocs" (fun _ ->
    executeFSIWithArgs "docs/tools" "generate.fsx" ["--define:RELEASE"] [] |> ignore
)

Target "ReleaseDocs" (fun _ ->
    let tempDocsDir = "temp/gh-pages"
    CleanDir tempDocsDir
    Repository.cloneSingleBranch "" (gitHome + "/" + gitName + ".git") "gh-pages" tempDocsDir

    fullclean tempDocsDir
    CopyRecursive "docs/output" tempDocsDir true |> tracefn "%A"
    StageAll tempDocsDir
    Commit tempDocsDir (sprintf "Update generated documentation for version %s" release.NugetVersion)
    Branches.push tempDocsDir
)

// --------------------------------------------------------------------------------------
// Run all targets by default. Invoke 'build <Target>' to override

Target "Default" DoNothing
Target "Release" DoNothing
Target "PrepareRelease" DoNothing
Target "Help" (fun _ -> PrintTargets() )

"Clean"
  ==> "AssemblyInfo"
  ==> "Build"
  ==> "RunTests"
  ==> "Default"


"Build"
  ==> "PrepareRelease"
  ==> "Nuget.Azure"
  ==> "Nuget.Standalone"
//  ==> "GenerateDocs"
//  ==> "ReleaseDocs"
  ==> "Release"

//// start build
RunTargetOrDefault "Default"