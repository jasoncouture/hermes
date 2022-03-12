using System;
using System.Linq;
using Journal.Journal;
using Xunit;

namespace Journal.UnitTests;

public class JournalDataTests
{
    [Fact]
    public void JournalData_ThrowsArgumentExceptionWhenSegmentTooSmall()
    {
        var data = new byte[4];
        var journalData = new JournalData(1234, data);
        Assert.Throws<ArgumentException>(() => journalData.WriteTo(data));
    }
    [Fact]
    public void JournalData_SizeMeetsExpectation()
    {
        var headerSize = sizeof(int) + sizeof(long);
        var arraySize = 4;
        var expectedSize = headerSize + arraySize;
        var buffer = new byte[arraySize];
        var entry = new JournalData(1234, buffer);
        Assert.Equal(expectedSize, entry.Size);
    }
    
    [Fact]
    public void JournalData_CanDecodeEncodedData()
    {
        var expected = new JournalData(1234L, Enumerable.Range(0, 32).Select(i => (byte) i).ToArray());
        var buffer = new byte[expected.Size];
        expected.WriteTo(buffer);
        var actual = JournalData.ReadFrom(9999, buffer);
        Assert.Equal(expected.Offset, actual.Offset);
        Assert.Equal(expected.Data.Count, actual.Data.Count);
        Assert.Equal(expected.Data, actual.Data);
    }
    
    [Fact]
    public void JournalData_UsesFileOffsetWhenOffsetDataIsZero()
    {
        var expected = new JournalData(0L, Enumerable.Range(0, 32).Select(i => (byte) i).ToArray());
        var buffer = new byte[expected.Size];
        expected.WriteTo(buffer);
        var actual = JournalData.ReadFrom(9999, buffer);
        Assert.NotEqual(expected.Offset, actual.Offset);
    }
}