using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FsCheck.Xunit;
using Xunit;

namespace Hermes.Cluster.UnitTests;

public class TopicIdManagerTests
{
    [Fact]
    public async Task TopicCanBeRetrievedAfterAssignCommand()
    {
        const string TopicName = "testTopic";
        var topicManager = new TopicIdManager();
        var commands = new TopicIdManagerCommand[]
        {
            new AssignTopicIdCommand(TopicName)
        };
        Assert.Throws<KeyNotFoundException>(() => topicManager.GetTopicId(TopicName));
        await topicManager.ApplyAsync(commands, CancellationToken.None);

        var id = topicManager.GetTopicId("testTopic");
        Assert.Equal(TopicName, topicManager.GetTopicName(id), StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task UniqueTopicIdsAssigned()
    {
        var commands = Enumerable.Range(0, 100).Select(i => $"testTopic{i}").Select(i => new AssignTopicIdCommand(i))
            .ToArray();
        var explicitAssignmentCommands = new[]
        {
            new AssignTopicIdCommand("manualTestTopic1", 3),
            new AssignTopicIdCommand("manualTestTopic2", 55)
        };
        var topicManager = new TopicIdManager();

        await topicManager.ApplyAsync(explicitAssignmentCommands, CancellationToken.None);
        await topicManager.ApplyAsync(commands, CancellationToken.None);

        Assert.Equal(explicitAssignmentCommands[0].Id!.Value,
            topicManager.GetTopicId(explicitAssignmentCommands[0].Topic));
        Assert.Equal(explicitAssignmentCommands[1].Id!.Value,
            topicManager.GetTopicId(explicitAssignmentCommands[1].Topic));

        var snapshot = await topicManager.GenerateSnapshot(CancellationToken.None);
        Assert.Equal(snapshot.Commands.Select(i => i.Topic).OrderBy(i => i),
            commands.Union(explicitAssignmentCommands).Select(i => i.Topic).OrderBy(i => i));
    }

    [Fact]
    public async Task SnapshotRestoreSetsStateCorrectly()
    {
        var restoreSnapshotCommand = new RestoreSnapshotCommand(new[]
        {
            new AssignTopicIdCommand("topic1", 1),
            new AssignTopicIdCommand("topic2", 2)
        });
        var topicManager = new TopicIdManager();
        await topicManager.ApplyAsync(restoreSnapshotCommand.Commands.First() with {Topic = "testTopic", Id = 3},
            CancellationToken.None);
        await topicManager.ApplyAsync(restoreSnapshotCommand, CancellationToken.None);
        
        Assert.False(topicManager.TryGetTopicId("testTopic", out _));
        foreach (var command in restoreSnapshotCommand.Commands)
        {
            Assert.True(topicManager.TryGetTopicId(command.Topic, out _));
        }
    }

    [Property]
    public void TopicManagerTryGetNeverThrowsExceptionsWhenTopicIsNotNull(string? topic)
    {
        if (topic is null) return;
        var topicManager = new TopicIdManager();
        Assert.False(topicManager.TryGetTopicId(topic, out _));
    }

    [Property]
    public void TopicManagerGetIdThrowsWhenKeyNotFound(string? topic)
    {
        if (topic is null) return;
        Assert.Throws<KeyNotFoundException>(() => new TopicIdManager().GetTopicId(topic));
    }

    [Property]
    public void TopicManagerGetNameThrowsKeyNotFound(long id)
    {
        Assert.Throws<KeyNotFoundException>(() => new TopicIdManager().GetTopicName(id));
    }
}