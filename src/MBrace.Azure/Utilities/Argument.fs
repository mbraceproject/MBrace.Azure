module MBrace.Azure.Runtime.Arguments

open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Runtime

// TODO replace with UnionArgParser
/// BASE64 serialized argument parsing schema
    
type Config = { 
    Configuration : Configuration
    MaxTasks : int
    Name : string option
}
with
    static member ToBase64Pickle (config : Config) =
        Config.Initialize(true)
        let pickle = VagabondRegistry.Instance.Serializer.Pickle(config)
        System.Convert.ToBase64String pickle

    static member OfBase64Pickle (args : string []) =
        Config.Initialize(true)
        let bytes = System.Convert.FromBase64String(args.[0])
        VagabondRegistry.Instance.Serializer.UnPickle<Config> bytes