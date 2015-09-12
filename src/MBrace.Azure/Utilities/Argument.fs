module MBrace.Azure.Runtime.Arguments

open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Runtime

// TODO replace with Argu
/// BASE64 serialized argument parsing schema
    
type Config = { 
    Configuration : Configuration
    MaxWorkItems : int
    Name : string option
    LogLevel : LogLevel option
}
with
    static member ToBase64Pickle (config : Config) =
        let pickle = VagabondRegistry.Instance.Serializer.Pickle(config)
        System.Convert.ToBase64String pickle

    static member OfBase64Pickle (args : string []) =
        let bytes = System.Convert.FromBase64String(args.[0])
        VagabondRegistry.Instance.Serializer.UnPickle<Config> bytes