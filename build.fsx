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
let authors = [ "Kostas Rontogiannis" ]

let description = """ Windows Azure implementation of the MBrace.Core. """

let tags = "F# cloud mapreduce distributed windowsazure"

// --------------------------------------------------------------------------------------
// Read release notes & version info from RELEASE_NOTES.md
Environment.CurrentDirectory <- __SOURCE_DIRECTORY__

let gitHome = "https://github.com/mbraceproject"
let gitName = "MBrace.Azure"

module Runtime =
    let release = parseReleaseNotes (IO.File.ReadAllLines "RELEASE_NOTES.md") 
    let nugetVersion = release.NugetVersion
module StoreBindings =
    let release = parseReleaseNotes (IO.File.ReadAllLines "STORE_BINDINGS_NOTES.md") 
    let nugetVersion = release.NugetVersion


// Generate assembly info files with the right version & up-to-date information
Target "AssemblyInfo" (fun _ ->
    let attributes =
        [| 
            Attribute.Title project
            Attribute.Product project
            Attribute.Company "Nessos Information Technologies"
            Attribute.Copyright "\169 Nessos Information Technologies."
            Attribute.Trademark "MBrace.Azure"
            Attribute.Version Runtime.release.AssemblyVersion
            Attribute.FileVersion Runtime.release.AssemblyVersion
        |]

    let store_attributes = 
        [| yield! attributes.[0..4]
           yield  Attribute.Version StoreBindings.release.AssemblyVersion
           yield  Attribute.FileVersion StoreBindings.release.AssemblyVersion
        |]

    !! "./src/**/AssemblyInfo.fs"
    |> Seq.iter (fun info -> CreateFSharpAssemblyInfo info attributes)
    !! "./samples/**/AssemblyInfo.fs"
    |> Seq.iter (fun info -> CreateFSharpAssemblyInfo info attributes)
    !! "./samples/**/AssemblyInfo.cs"
    |> Seq.iter (fun info -> CreateCSharpAssemblyInfo info attributes)
    
    !! "./src/MBrace.Azure.Store/AssemblyInfo.fs"
    |> Seq.iter (fun info -> CreateFSharpAssemblyInfo info store_attributes)
)


// --------------------------------------------------------------------------------------
// Clean and restore packages

Target "RestorePackages" (fun _ ->
    !! "./**/packages.config"
    |> Seq.iter (RestorePackage (fun p -> { p with ToolPath = "./.nuget/NuGet.exe" }))
)

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
        yield "bin/MBrace.Azure.Runtime.Tests.dll"
    ]

Target "RunTests" (fun _ ->
    let nunitVersion = GetPackageVersion "packages" "NUnit.Runners"
    let nunitPath = sprintf "packages/NUnit.Runners.%s/tools" nunitVersion
    ActivateFinalTarget "CloseTestRunner"

    testAssemblies
    |> NUnit (fun p -> 
        { p with
            DisableShadowCopy = true
            ToolPath = nunitPath
            Framework = "4.5"
            TimeOut = TimeSpan.FromMinutes 60. })
)

FinalTarget "CloseTestRunner" (fun _ ->  
    ProcessHelper.killProcess "nunit-agent.exe"
)

// Nuget packages

let addFile (target : string) (file : string) =
    if File.Exists (Path.Combine("nuget", file)) then (file, Some target, None)
    else raise <| new FileNotFoundException(file)

let addAssembly (target : string) assembly =
    let includeFile force file =
        let file = file
        if File.Exists (Path.Combine("nuget", file)) then [(file, Some target, None)]
        elif force then raise <| new FileNotFoundException(file)
        else []

    seq {
        yield! includeFile true assembly
        yield! includeFile false <| Path.ChangeExtension(assembly, "pdb")
        yield! includeFile false <| Path.ChangeExtension(assembly, "xml")
        yield! includeFile false <| assembly + ".config"
    }

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
    Commit tempDocsDir (sprintf "Update generated documentation for version %s" Runtime.release.NugetVersion)
    Branches.push tempDocsDir
)

// --------------------------------------------------------------------------------------
// Run all targets by default. Invoke 'build <Target>' to override

Target "Default" DoNothing
Target "Release" DoNothing
Target "PrepareRelease" DoNothing
Target "NuGet" DoNothing
Target "Help" (fun _ -> PrintTargets() )

"Clean"
  ==> "RestorePackages"
  ==> "AssemblyInfo"
  ==> "Build"
  ==> "RunTests"
  ==> "Default"

"Build"
  ==> "PrepareRelease"
  ==> "Nuget"
//  ==> "GenerateDocs"
//  ==> "ReleaseDocs"
  ==> "Release"

//// start build
RunTargetOrDefault "Build"
