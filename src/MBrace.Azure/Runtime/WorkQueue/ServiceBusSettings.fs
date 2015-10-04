namespace MBrace.Azure.Runtime

open System
open MBrace.Runtime

/// Blob payload of serialized work items 
type MessagePayload =
    | Single of CloudWorkItem
    | Batch of CloudWorkItem []

/// Common settings for queue, topic and messages.
type internal ServiceBusSettings private () = 
    /// Max number of deliveries before deadlettering.
    static member MaxDeliveryCount = Int32.MaxValue
    /// Message lock renew interval (in milliseconds).
    static member RenewLockInverval = 10000 
    /// Maximum message lock duration (5 minutes is the max value).
    static member MaxLockDuration = TimeSpan.FromSeconds(60.)
    /// Maximum message TTL.
    static member MaxTTL = TimeSpan.MaxValue
    /// Server wait time for dequeue.
    static member ServerWaitTime = TimeSpan.FromMilliseconds(50.)
    /// Affinity.
    static member AffinityProperty = "worker"
    /// ParentTaskId.
    static member ParentTaskIdProperty = "parentId"
    /// BatchIndex.
    static member BatchIndexProperty = "batchIndex"
    /// WorkItemId.
    static member WorkItemIdProperty = "uuid"
    /// Delivery count if formerly topic
    static member TopicDeliveryCount = "topicDeliveryCount"
    /// SUbscription queue auto delete interval.
    static member SubscriptionAutoDeleteInterval = TimeSpan.MaxValue 