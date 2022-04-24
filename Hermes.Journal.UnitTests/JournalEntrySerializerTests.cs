using System;
using System.Collections.Immutable;
using System.Linq;
using Xunit;

namespace Hermes.Journal.UnitTests;

public class JournalEntrySerializerTests
{
    [Fact]
    public void JournalEntryCanDeserializeSerializedData()
    {
        var serializer = new JournalEntrySerializer();
        var expected = new JournalEntry(1, 2, Guid.NewGuid().ToByteArray(), Guid.NewGuid().ToByteArray(), new[]
        {
            new JournalEntryHeader("key1", new[] {Guid.NewGuid().ToString("n")}),
            new JournalEntryHeader("key2", new[] {Guid.NewGuid().ToString("n"), Guid.NewGuid().ToString("n")})
        });
        using var buffer = serializer.Serialize(expected);
        var actual = serializer.Deserialize(buffer.ArraySegment);

        AssertJournalEntriesEqual(expected, actual);
    }

    private void AssertJournalEntriesEqual(JournalEntry expected, JournalEntry actual)
    {
        Assert.Equal(expected.SequenceNumber, actual.SequenceNumber);
        Assert.Equal(expected.Partition, actual.Partition);
        Assert.Equal(expected.Key, actual.Key);
        Assert.Equal(expected.Data, actual.Data);
        var zippedHeaders = expected.JournalEntryHeaders.Zip(actual.JournalEntryHeaders);
        foreach (var entry in zippedHeaders)
        {
            Assert.Equal(entry.First.Key, entry.Second.Key);
            Assert.Equal(entry.First.Values, entry.Second.Values);
        }
    }

    [Fact]
    public void JournalEntrySerializerCanHandleEmptyHeaders()
    {
        var serializer = new JournalEntrySerializer();
        var expected = new JournalEntry(1, 2, Guid.NewGuid().ToByteArray(), Guid.NewGuid().ToByteArray(),
            Array.Empty<JournalEntryHeader>());
        using var buffer = serializer.Serialize(expected);
        var actual = serializer.Deserialize(buffer.ArraySegment);

        Assert.Equal(expected.SequenceNumber, actual.SequenceNumber);
        Assert.Equal(expected.Partition, actual.Partition);
        Assert.Equal(expected.Key, actual.Key);
        Assert.Equal(expected.Data, actual.Data);
        Assert.Empty(actual.JournalEntryHeaders);
    }

    [Fact]
    public void JournalSerializerCanHandleNullKeysAndValues()
    {
        var serializer = new JournalEntrySerializer();
        var expected = new JournalEntry(1, 2, null, null, Array.Empty<JournalEntryHeader>());
        using var buffer = serializer.Serialize(expected);
        var actual = serializer.Deserialize(buffer.ArraySegment);

        AssertJournalEntriesEqual(expected, actual);

        Assert.Empty(actual.JournalEntryHeaders);
    }

    [Fact]
    public void TotalEntrySizeIsWrittenToFirstFourBytes()
    {
        var serializer = new JournalEntrySerializer();
        var expected = new JournalEntry(1, 2, null, null, Array.Empty<JournalEntryHeader>());
        using var buffer = serializer.Serialize(expected);

        var writtenSize = BitConverter.ToInt32(buffer.ArraySegment);

        Assert.Equal(buffer.Count, writtenSize);
    }

    [Fact]
    public void VersionZeroIsWrittenToFifthByte()
    {
        var serializer = new JournalEntrySerializer();
        var expected = new JournalEntry(1, 2, null, null, Array.Empty<JournalEntryHeader>());
        const byte ExpectedVersion = 0;
        using var buffer = serializer.Serialize(expected);
        var versionByte = buffer.ArraySegment[4];

        Assert.Equal(ExpectedVersion, versionByte);
    }

    [Fact]
    public void TryDeserializeReturnsFalseWhenArrayIsTooSmall()
    {
        var serializer = new JournalEntrySerializer();
        const bool expected = false;
        // The success case is tested via Deserialize.
        // If these methods are split, add a test for TryDeserialize
        var result = serializer.TryDeserialize(ArraySegment<byte>.Empty, out _);

        Assert.Equal(expected, result);
    }

    [Fact]
    public void SerializerCanWriteLargePayloads()
    {
        var serializer = new JournalEntrySerializer();

        var expected = CreateLargeEntry();
        using var buffer = serializer.Serialize(expected);
        var actual = serializer.Deserialize(buffer.ArraySegment);

        AssertJournalEntriesEqual(expected, actual);
    }

    private JournalEntry CreateLargeEntry()
    {
        var headers = Enumerable.Range(0, 25).Select(i => new JournalEntryHeader(i.ToString(),
            Enumerable.Range(0, 3).Select(x => Guid.NewGuid().ToString("n")).ToArray())).ToImmutableArray();
        using var buffer = BorrowedArray<byte>.Rent(1024 * 1024 * 5);
        return new JournalEntry(1, 2, Guid.NewGuid().ToByteArray(), buffer.Array, headers);
    }
}