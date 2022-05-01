namespace Hermes.Cluster;

public sealed record AssignTopicIdCommand(string Topic, long? Id = null) : TopicIdManagerCommand(nameof(AssignTopicIdCommand));