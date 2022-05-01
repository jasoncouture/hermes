namespace Hermes.Cluster;

public sealed record DeleteTopicCommand(string Topic) : TopicIdManagerCommand(nameof(DeleteTopicCommand));