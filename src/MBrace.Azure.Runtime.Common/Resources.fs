namespace Nessos.MBrace.Azure.Runtime.Resources

open System
open System.Collections.Concurrent
open System.Runtime.Serialization
open Microsoft.WindowsAzure.Storage
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Runtime.Common
open Nessos.MBrace.Runtime
open Microsoft.WindowsAzure.Storage.Table

type ResourceFactory private (config : Configuration) =
  
    //do ConfigurationRegistry.Register<_>(config.ConfigurationId, new WorkerMonitor(config.ConfigurationId, config.DefaultTableOrContainer))
    //do ConfigurationRegistry.Register<_>(config.ConfigurationId, new ProcessMonitor(config.ConfigurationId, config.DefaultTableOrContainer))
    //do ConfigurationRegistry.Register<_>(config.ConfigurationId, new NullLogger() :> ILogger)

    member __.RequestCounter(container, count) = Counter.Create(config.ConfigurationId, container, count)
    member __.RequestResultAggregator<'T>(container, count : int) = ResultAggregator<'T>.Create(config.ConfigurationId, container, count)
    member __.RequestCancellationTokenSource(container, ?parent) = DistributedCancellationTokenSource.Create(config.ConfigurationId, container, ?parent = parent)
    member __.RequestResultCell<'T>(container) = ResultCell<Result<'T>>.Create(config.ConfigurationId, container)
    member __.RequestProcessLogger(container, pid) = 
        // TODO : change
        let logger = new ProcessLogger(config.ConfigurationId, container, ProcessLog(id = pid)) 
        logger.Attach(new ConsoleLogger())
        logger 
    //member __.ProcessMonitor = ConfigurationRegistry.Resolve<ProcessMonitor>(config.ConfigurationId)
    //member __.WorkerMonitor = ConfigurationRegistry.Resolve<WorkerMonitor>(config.ConfigurationId)
    //member __.Logger = ConfigurationRegistry.Resolve<ILogger>(config.ConfigurationId)
    
    //member __.RegisterLocalSubscription(subscription) = ConfigurationRegistry.Register<Subscription>(config.ConfigurationId, subscription)
    //member __.LocalSubscription = ConfigurationRegistry.Resolve<Subscription>(config.ConfigurationId)

    static member Create (config : Configuration) = new ResourceFactory(config)