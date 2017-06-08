// --------------------------------------------------------------------------------------
// FAKE build script 
// --------------------------------------------------------------------------------------

#I "packages/build/FAKE/tools"
#r "packages/build/FAKE/tools/FakeLib.dll"

open Fake
open Fake.AppVeyor
open Fake.Git
open Fake.AssemblyInfoFile
open Fake.ReleaseNotesHelper

open System
open System.IO

let project = "MBrace.Azure"

// --------------------------------------------------------------------------------------
// Read release notes & version info from RELEASE_NOTES.md
Environment.CurrentDirectory <- __SOURCE_DIRECTORY__
let gitHash = Information.getCurrentHash()
let buildDate = DateTime.UtcNow
let release = LoadReleaseNotes "RELEASE_NOTES.md"
let isAppVeyorBuild = buildServer = BuildServer.AppVeyor
let isVersionTag tag = Version.TryParse tag |> fst
let hasRepoVersionTag = isAppVeyorBuild && AppVeyorEnvironment.RepoTag && isVersionTag AppVeyorEnvironment.RepoTagName
let assemblyVersion = if hasRepoVersionTag then AppVeyorEnvironment.RepoTagName else release.NugetVersion
let buildVersion =
    if hasRepoVersionTag then assemblyVersion
    else if isAppVeyorBuild then sprintf "%s-b%s" assemblyVersion AppVeyorEnvironment.BuildNumber
    else assemblyVersion

let gitOwner = "mbraceproject"
let gitHome = "https://github.com/" + gitOwner
let gitName = "MBrace.Azure"

Target "BuildVersion" (fun _ ->
    Shell.Exec("appveyor", sprintf "UpdateBuild -Version \"%s\"" buildVersion) |> ignore
)

// Generate assembly info files with the right version & up-to-date information
Target "AssemblyInfo" (fun _ ->
    let attributes =
        [| 
            Attribute.Product "MBrace.Azure"
            Attribute.Company "Nessos Information Technologies"
            Attribute.Copyright "\169 Nessos Information Technologies."
            Attribute.Trademark "MBrace"
            Attribute.Metadata("Release Signature", 
                sprintf "Version %s, Git Hash %s, Build Date %s" 
                    assemblyVersion
                    gitHash 
                    (buildDate.ToString "ddMMyyyy HH:mm zzz"))
            Attribute.Version assemblyVersion
            Attribute.FileVersion assemblyVersion
        |]

    !! "./src/**/AssemblyInfo.fs" |> Seq.iter (fun infoFile -> 
        CreateFSharpAssemblyInfo infoFile attributes
        let infoFileText = File.ReadAllText infoFile
        let infoFileText = infoFileText + "\r\n    let [<Literal>] ReleaseTag = \"" + release.NugetVersion + "\"\r\n" 
        File.WriteAllText(infoFile,infoFileText))
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

let csdefTemplate = "src" @@ "MBrace.Azure.CloudService" @@ "ServiceDefinition.csdef"
let csdefForSize size = "src" @@ "MBrace.Azure.CloudService" @@ "ServiceDefinition" + size + ".csdef"
let cspkgAfterBuild configuration = "bin" @@ "cspkg" @@ "app.publish" @@ "MBrace.Azure.CloudService.cspkg"
let cspkgAfterCopy size = "bin" @@ "cspkg" @@ "MBrace.Azure.CloudService-" + size + ".cspkg"

// See https://azure.microsoft.com/en-gb/documentation/articles/cloud-services-sizes-specs/
let vmSizes = 
    [ yield "Small"; 
      yield "Medium"; 
      yield "Large";
      yield "ExtraLarge";

      for i in 5 .. 11 -> "A" + string i; 
      for i in 1 .. 4 -> "Standard_D" + string i 
      for i in 11 .. 14 -> "Standard_D" + string i 
      for i in 1 .. 5 -> "Standard_D" + string i + "_v2" 
      for i in 11 .. 14 -> "Standard_D" + string i + "_v2" 
    ]

Target "Build" (fun _ ->
    // Build the rest of the project
    { BaseDirectory = __SOURCE_DIRECTORY__
      Includes = [ project + ".sln" ]
      Excludes = [] } 
    |> MSBuild "" "Build" ["Configuration", configuration]
    |> Log "AppBuild-Output: "
)

// Build lots of packages for differet VM sizes
Target "BuildPackages" (fun _ ->
    for size in vmSizes do
        csdefTemplate |> CopyFile (csdefForSize size)
        (csdefForSize size) |> ReplaceInFile (fun s -> s.Replace("vmsize=\"Large\"", "vmsize=\"" + size + "\"" ))
        { BaseDirectory = __SOURCE_DIRECTORY__
          Includes = [ "src" @@ "MBrace.Azure.CloudService" @@ "MBrace.Azure.CloudService.ccproj" ]
          Excludes = [] } 
        |> MSBuild "" "Publish" ["Configuration", configuration + "_AzureSDK"; "ServiceVMSize", size]
        |> Log "AppPackage-Output: "
        (cspkgAfterBuild configuration) |> CopyFile (cspkgAfterCopy size)

)

// --------------------------------------------------------------------------------------
// Run the unit tests using test runner & kill test runner when complete


let testAssemblies = 
    [
        "bin/MBrace.Azure.Tests.dll"
    ]

Target "RunTests" (fun _ ->
    let nunitVersion = GetPackageVersion "packages" "NUnit.Runners"
    let nunitPath = "packages/NUnit.Runners/tools" 
    ActivateFinalTarget "CloseTestRunner"

    testAssemblies
    |> NUnitSequential.NUnit (fun p -> 
        { p with
            DisableShadowCopy = false
            ToolPath = nunitPath
            Framework = "4.5"
            IncludeCategory = "Standalone Cluster"
            TimeOut = TimeSpan.FromMinutes 60. })
)

FinalTarget "CloseTestRunner" (fun _ ->  
    ProcessHelper.killProcess "nunit-agent.exe"
)

//// --------------------------------------------------------------------------------------
//// Build a NuGet package


Target "NuGet" (fun _ ->    
    Paket.Pack (fun p -> 
        { p with 
            ToolPath = ".paket/paket.exe" 
            OutputPath = "bin/"
            Version = release.NugetVersion
            ReleaseNotes = toLines release.Notes })
)

Target "NuGetPush" (fun _ -> Paket.Push (fun p -> { p with WorkingDir = "bin/" }))

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


#load "Octokit.fsx"
open Octokit

Target "ReleaseGithub" (fun _ ->
    let remote =
        Git.CommandHelper.getGitResult "" "remote -v"
        |> Seq.filter (fun (s: string) -> s.EndsWith("(push)"))
        |> Seq.tryFind (fun (s: string) -> s.Contains(gitOwner + "/" + gitName))
        |> function None -> gitHome + "/" + gitName | Some (s: string) -> s.Split().[0]

    //StageAll ""
    Git.Commit.Commit "" (sprintf "Bump version to %s" release.NugetVersion)
    Branches.pushBranch "" remote (Information.getBranchName "")

    Branches.tag "" release.NugetVersion
    Branches.pushTag "" remote release.NugetVersion

    let client =
        match Environment.GetEnvironmentVariable "OctokitToken" with
        | null -> 
            let user =
                match getBuildParam "github-user" with
                | s when not (String.IsNullOrWhiteSpace s) -> s
                | _ -> getUserInput "Username: "
            let pw =
                match getBuildParam "github-pw" with
                | s when not (String.IsNullOrWhiteSpace s) -> s
                | _ -> getUserPassword "Password: "

            createClient user pw
        | token -> createClientWithToken token

    // release on github
    client
    |> createDraft gitOwner gitName release.NugetVersion (release.SemVer.PreRelease <> None) release.Notes 
    |> uploadFiles (Seq.map cspkgAfterCopy vmSizes)
    |> releaseDraft
    |> Async.RunSynchronously
)


// --------------------------------------------------------------------------------------
// Run all targets by default. Invoke 'build <Target>' to override

Target "Default" DoNothing
Target "Release" DoNothing
Target "Help" (fun _ -> PrintTargets() )

"Clean"
  =?> ("BuildVersion", isAppVeyorBuild)
  ==> "AssemblyInfo"
  ==> "Build"
  =?> ("RunTests", not isAppVeyorBuild) // testing not yet enabled on appveyor because Azure resource access is needed
  ==> "Default"

"Build"
  ==> "BuildPackages"
  ==> "NuGet"
  ==> "NuGetPush"
  ==> "ReleaseGithub"
  ==> "Release"

//// start build
RunTargetOrDefault "Default"