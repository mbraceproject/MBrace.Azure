// --------------------------------------------------------------------------------------
// FAKE build script 
// --------------------------------------------------------------------------------------

#I "packages/build/FAKE/tools"
#r "packages/build/FAKE/tools/FakeLib.dll"

open Fake
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
let release = parseReleaseNotes (IO.File.ReadAllLines "RELEASE_NOTES.md") 
let nugetVersion = release.NugetVersion

let gitOwner = "mbraceproject"
let gitHome = "https://github.com/" + gitOwner
let gitName = "MBrace.Azure"

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
                    release.AssemblyVersion
                    gitHash 
                    (buildDate.ToString "ddMMyyyy HH:mm zzz"))
            Attribute.Version release.AssemblyVersion
            Attribute.FileVersion release.AssemblyVersion
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
let vmWorkerZip = "bin" @@ "MBrace.Azure.zip"
let webjobZip = "bin" @@ "MBrace.Azure.Worker.zip"
let azureDeployTemplate = "src" @@ "MBrace.Azure.ResourceManager" @@ "azuredeploy.template.json"
let generatedAzureDeploy = "deployment" @@ "azuredeploy.json"

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

let filesForZip =
    [ 
        "Argu.dll"
        "FSharp.Core.dll"
        "FSharp.Core.xml"
        "FsPickler.dll"
        "FsPickler.Json.dll"
        "MBrace.Azure.dll"
        "MBrace.Azure.pdb"
        "MBrace.Azure.XML"
        "mbrace.azureworker.exe"
        "mbrace.azureworker.exe.config"
        "MBrace.Core.dll"
        "MBrace.Core.pdb"
        "MBrace.Core.xml"
        "MBrace.Runtime.dll"
        "MBrace.Runtime.pdb"
        "MBrace.Runtime.xml"
        "Microsoft.Azure.KeyVault.Core.dll"
        "Microsoft.Data.Edm.dll"
        "Microsoft.Data.OData.dll"
        "Microsoft.Data.Services.Client.dll"
        "Microsoft.ServiceBus.dll"
        "Microsoft.WindowsAzure.Storage.dll"
        "Mono.Cecil.dll"
        "Newtonsoft.Json.dll"
        "System.Collections.Immutable.dll"
        "System.Reflection.Metadata.dll"
        "System.Spatial.dll"
        "Vagabond.AssemblyParser.dll"
        "Vagabond.dll"
    ]

// Build lots of packages for differet VM sizes and deployment methods
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
    
    CreateDir "bin/app_data/jobs/continuous/MBraceWorker"
    filesForZip
    |> List.iter (fun file -> CopyFile ("bin/app_data/jobs/continuous/MBraceWorker" @@ file) ("bin" @@ file))
    ZipHelper.CreateZip "bin" webjobZip "" 0 false (filesForZip |> List.map ((@@) "bin/app_data/jobs/continuous/MBraceWorker"))
    ZipHelper.CreateZip "bin" vmWorkerZip "" 0 false (filesForZip |> List.map ((@@) "bin"))
    azureDeployTemplate |> CopyFile generatedAzureDeploy
    generatedAzureDeploy |> ReplaceInFile (fun s -> s.Replace("<VERSION_NUMBER>", release.NugetVersion))
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
    |> NUnit (fun p -> 
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

Target "ReleaseNuGet" (fun _ -> Paket.Push (fun p -> { p with WorkingDir = "bin/" }))

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
    |> uploadFiles (["deployment" @@ "azuredeploy.json"; "deployment" @@ "tiny-cluster.json"; "deployment" @@ "small-cluster.json"; "deployment" @@ "medium-cluster.json"; "deployment" @@ "large-cluster.json"; vmWorkerZip; webjobZip])
    |> releaseDraft
    |> Async.RunSynchronously
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
  ==> "BuildPackages"
  ==> "PrepareRelease"
  ==> "NuGet"
  ==> "ReleaseNuGet"
  ==> "ReleaseGithub"
  ==> "Release"

//// start build
RunTargetOrDefault "Default"