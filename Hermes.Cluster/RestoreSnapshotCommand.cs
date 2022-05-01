namespace Hermes.Cluster;

public sealed record RestoreSnapshotCommand(IEnumerable<AssignTopicIdCommand> Commands) : TopicIdManagerCommand(nameof(RestoreSnapshotCommand));