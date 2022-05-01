namespace Hermes.Cluster;

public class TopicIdManager
{
    private SemaphoreSlim _assignmentSemaphore = new SemaphoreSlim(1, 1);
    private Dictionary<string, long> _topicIdAssignments = new(StringComparer.OrdinalIgnoreCase);
    private Dictionary<long, string> _reverseTopicIdAssignments = new();
    private long _nextTopicId = 0;

    private long GenerateTopicId() => Interlocked.Increment(ref _nextTopicId);

    public string GetTopicName(long topicId) =>
        _reverseTopicIdAssignments.GetValueOrDefault(topicId) ?? throw new KeyNotFoundException();

    public long GetTopicId(string topicName)
    {
        if (_topicIdAssignments.TryGetValue(topicName, out var topicId)) return topicId;
        throw new KeyNotFoundException();
    }

    public bool TryGetTopicId(string name, out long id) => _topicIdAssignments.TryGetValue(name, out id);

    private async ValueTask AssignTopicIdAsync(string topicName, long topicId, CancellationToken cancellationToken)
    {
        await _assignmentSemaphore.WaitAsync(cancellationToken);
        try
        {
            if (TryGetTopicId(topicName, out var id))
            {
                if (id == topicId) return;
            }

            _topicIdAssignments[topicName] = topicId;
            _reverseTopicIdAssignments[topicId] = topicName;
        }
        finally
        {
            _assignmentSemaphore.Release();
        }
    }

    private async ValueTask<long> AssignTopicIdAsync(string topicName, CancellationToken cancellationToken)
    {
        if (TryGetTopicId(topicName, out var id)) return id;
        await _assignmentSemaphore.WaitAsync(cancellationToken);
        try
        {
            if (TryGetTopicId(topicName, out id)) return id;
            do
            {
                id = GenerateTopicId() & long.MaxValue;
                if (!_reverseTopicIdAssignments.ContainsKey(id)) break;
            } while (true);

            _topicIdAssignments[topicName] = id;
            _reverseTopicIdAssignments[id] = topicName;

            return id;
        }
        finally
        {
            _assignmentSemaphore.Release();
        }
    }

    private async ValueTask DeleteTopicAsync(string topicName, CancellationToken cancellationToken)
    {
        if (!TryGetTopicId(topicName, out _)) return;
        await _assignmentSemaphore.WaitAsync(cancellationToken);
        try
        {
            if (!TryGetTopicId(topicName, out var id)) return;
            _topicIdAssignments.Remove(topicName);
            _reverseTopicIdAssignments.Remove(id);
        }
        finally
        {
            _assignmentSemaphore.Release();
        }
    }

    public async ValueTask ApplyAsync(IEnumerable<TopicIdManagerCommand> commands, CancellationToken cancellationToken)
    {
        foreach (var command in commands)
        {
            await ApplyAsync(command, cancellationToken);
        }
    }

    public async ValueTask ApplyAsync(TopicIdManagerCommand command, CancellationToken cancellationToken)
    {
        switch (command)
        {
            case AssignTopicIdCommand assignTopicIdCommand:
                if (assignTopicIdCommand.Id is null)
                {
                    await AssignTopicIdAsync(assignTopicIdCommand.Topic, cancellationToken);
                }
                else
                {
                    await AssignTopicIdAsync(assignTopicIdCommand.Topic, assignTopicIdCommand.Id.Value,
                        cancellationToken);
                }
                break;
            case DeleteTopicCommand deleteTopicCommand:
                await DeleteTopicAsync(deleteTopicCommand.Topic, cancellationToken);
                break;
            case RestoreSnapshotCommand restoreSnapshotCommand:
                // Before restore, we need to reset our state
                // No lock is needed here, since writes always flow through here.
                _topicIdAssignments.Clear();
                _reverseTopicIdAssignments.Clear();
                Interlocked.Exchange(ref _nextTopicId, 0L);
                await ApplyAsync(restoreSnapshotCommand.Commands, cancellationToken);
                break;
        }
    }

    public async ValueTask<RestoreSnapshotCommand> GenerateSnapshot(CancellationToken cancellationToken)
    {
        IEnumerable<KeyValuePair<string, long>> snapshot;
        await _assignmentSemaphore.WaitAsync(cancellationToken);
        try
        {
            snapshot = _topicIdAssignments.ToArray();
        }
        finally
        {
            _assignmentSemaphore.Release();
        }

        return new RestoreSnapshotCommand(snapshot.Select(i => new AssignTopicIdCommand(i.Key, i.Value)).ToArray());
    }
}