using System;
using System.Linq;
using Journal.Journal.Remote.Protocol;
using Xunit;

namespace Journal.UnitTests;

public class JournalCommandTests
{
    [Fact]
    public void JournalCommand_CanEncodeAndDecodeProperly()
    {
        var expectedCommand = new RemoteJournalCommand(1, RemoteJournalCommandType.WriteEntry,
            new[] {new ArraySegment<byte>(new byte[] {0, 1, 2, 3})});
        var buffer = new byte[expectedCommand.Size];
        expectedCommand.WriteTo(buffer);
        var actualCommand = RemoteJournalCommand.ReadFrom(buffer);

        Assert.Equal(expectedCommand.Size, actualCommand.Size);
        Assert.Equal(expectedCommand.Type, actualCommand.Type);
        Assert.Equal(expectedCommand.StreamId, actualCommand.StreamId);
        Assert.Equal(expectedCommand.Segments.First().Array, actualCommand.Segments.First().Array);
    }

    [Fact]
    public void JournalCommand_CorrectlyIdentifiesIncompleteData()
    {
        const int expectedRequiredSizeWhenLengthTooShort = sizeof(int);
        var expectedCommand = new RemoteJournalCommand(1, RemoteJournalCommandType.WriteEntry,
            new[] {new ArraySegment<byte>(new byte[] {0, 1, 2, 3})});
        var buffer = new ArraySegment<byte>(new byte[expectedCommand.Size]);
        expectedCommand.WriteTo(buffer);
        Assert.False(RemoteJournalCommand.IsCompletePacket(buffer.Slice(0, 3), out var requiredSize));
        Assert.Equal(expectedRequiredSizeWhenLengthTooShort, requiredSize);
        Assert.False(RemoteJournalCommand.IsCompletePacket(buffer.Slice(0, 6), out requiredSize));
        Assert.Equal(expectedCommand.Size, requiredSize);
        Assert.True(RemoteJournalCommand.IsCompletePacket(buffer, out requiredSize));
        Assert.Equal(expectedCommand.Size, requiredSize);
    }
}