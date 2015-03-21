#I "../../lib/"
#I "../../bin"

#r "MBrace.Core.dll"
#r "MBrace.Library.dll"
#r "FsPickler.dll"
#r "Vagrant.dll"
#r "MBrace.Azure.Runtime.Common.dll"
#r "MBrace.Azure.Runtime.dll"
#r "MBrace.Azure.Client.dll"
#r "Streams.Core.dll"
#r "MBrace.Streams.dll"

let private selectEnv name =
    (System.Environment.GetEnvironmentVariable(name,System.EnvironmentVariableTarget.User),
        System.Environment.GetEnvironmentVariable(name,System.EnvironmentVariableTarget.Machine))
    |> function | null, s | s, null | s, _ -> s

let azureStorageConn = selectEnv "azurestorageconn"

let serviceBusConn = selectEnv "azureservicebusconn"

(*
#r "MBrace.Azure.Runtime.Standalone"
open Nessos.MBrace.Azure.Runtime.Standalone
Runtime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../lib/MBrace.Azure.Runtime.Standalone.exe"
Runtime.Spawn(config, 4)

runtime.ClientLogger.Attach(Common.ConsoleLogger())
*)



open System
open System.IO
open System.Text.RegularExpressions


/// words ignored by wordcount
let noiseWords = 
    set [
        "a"; "about"; "above"; "all"; "along"; "also"; "although"; "am"; "an"; "any"; "are"; "aren't"; "as"; "at";
        "be"; "because"; "been"; "but"; "by"; "can"; "cannot"; "could"; "couldn't"; "did"; "didn't"; "do"; "does"; 
        "doesn't"; "e.g."; "either"; "etc"; "etc."; "even"; "ever";"for"; "from"; "further"; "get"; "gets"; "got"; 
        "had"; "hardly"; "has"; "hasn't"; "having"; "he"; "hence"; "her"; "here"; "hereby"; "herein"; "hereof"; 
        "hereon"; "hereto"; "herewith"; "him"; "his"; "how"; "however"; "I"; "i.e."; "if"; "into"; "it"; "it's"; "its";
        "me"; "more"; "most"; "mr"; "my"; "near"; "nor"; "now"; "of"; "onto"; "other"; "our"; "out"; "over"; "really"; 
        "said"; "same"; "she"; "should"; "shouldn't"; "since"; "so"; "some"; "such"; "than"; "that"; "the"; "their"; 
        "them"; "then"; "there"; "thereby"; "therefore"; "therefrom"; "therein"; "thereof"; "thereon"; "thereto"; 
        "therewith"; "these"; "they"; "this"; "those"; "through"; "thus"; "to"; "too"; "under"; "until"; "unto"; "upon";
        "us"; "very"; "viz"; "was"; "wasn't"; "we"; "were"; "what"; "when"; "where"; "whereby"; "wherein"; "whether";
        "which"; "while"; "who"; "whom"; "whose"; "why"; "with"; "without"; "would"; "you"; "your" ; "have"; "thou"; "will"; 
        "shall"
    ]

// Splits a string into words
let splitWords =
    let regex = new Regex(@"[\W]+", RegexOptions.Compiled)
    fun text -> regex.Split(text)

let wordTransform (word : string) = word.Trim().ToLower()

let wordFilter (word : string) = word.Length > 3 && not <| noiseWords.Contains(word)


