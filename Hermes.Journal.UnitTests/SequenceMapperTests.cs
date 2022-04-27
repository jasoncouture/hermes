using System;
using System.Collections.Generic;
using FsCheck.Xunit;
using Xunit;

namespace Hermes.Journal.UnitTests;

public class SequenceMapperTests
{
    [Fact]
    public void ValueStoredAtPartitionSequenceCanBeRetrieved()
    {
        var sequenceMapper = new SequenceMapper();
        const long sequence = 100;
        const long expectedValue = 1000;

        sequenceMapper.SetPhysicalOffset(sequence, expectedValue);
        Assert.Equal(expectedValue, sequenceMapper.GetPhysicalOffset(sequence));
    }

    [Property]
    public void SequenceMapperThrowsArgumentExceptionWhenSequenceIsNegative(long sequence)
    {
        if (sequence >= 0) return;
        var sequenceMapper = new SequenceMapper();
        Assert.Throws<ArgumentException>(() => sequenceMapper.SetPhysicalOffset(sequence, 1));
        Assert.Throws<ArgumentException>(() => sequenceMapper.GetPhysicalOffset(sequence));
    }

    [Property]
    public void SequenceMapperThrowsArgumentExceptionWhenOffsetIsNegative(long offset)
    {
        if (offset >= 0) return;
        var sequenceMapper = new SequenceMapper();
        Assert.Throws<ArgumentException>(() => sequenceMapper.SetPhysicalOffset(0, offset));
    }

    [Fact]
    public void InsertingDuplicateKeyThrowsInvalidOperationException()
    {
        var sequenceMapper = new SequenceMapper();
        sequenceMapper.SetPhysicalOffset(0, 1);
        Assert.Equal(1, sequenceMapper.GetPhysicalOffset(0));
        Assert.Throws<InvalidOperationException>(() => sequenceMapper.SetPhysicalOffset(0, 2));
        Assert.Equal(1, sequenceMapper.GetPhysicalOffset(0));
    }

    [Fact]
    public void RemoveOffsetsBeforeRemovesKnownOffsetsLessThanProvidedValue()
    {
        var sequenceMapper = new SequenceMapper();
        const int expectedTotalCount = 50;
        for (var x = 0; x < 100; x++)
        {
            sequenceMapper.SetPhysicalOffset(x, x);
        }

        sequenceMapper.RemoveOffsetsBefore(50);
        Assert.Equal(expectedTotalCount, sequenceMapper.Count);

        for (var x = 0; x < 50; x++)
        {
            Assert.Throws<KeyNotFoundException>(() => sequenceMapper.GetPhysicalOffset(x));
        }

        for (var x = 50; x < 100; x++)
        {
            Assert.Equal((long) x, sequenceMapper.GetPhysicalOffset(x));
        }

        Assert.Throws<KeyNotFoundException>(() => sequenceMapper.GetPhysicalOffset(100));
    }

    [Fact]
    public void RemovingPhysicalOffsetRemovesOnlyThatOffset()
    {
        var sequenceMapper = new SequenceMapper();
        var expected = new[] {0L, 2L};
        for (var x = 0; x < 3; x++)
        {
            sequenceMapper.SetPhysicalOffset(x, x);
        }

        sequenceMapper.RemovePhysicalOffset(1);
        Assert.Equal(2, sequenceMapper.Count);
        Assert.Equal(expected, sequenceMapper.GetSequences());
    }
}