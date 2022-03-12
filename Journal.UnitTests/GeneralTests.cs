using System;
using Journal.Encoding;
using NuGet.Frameworks;
using Xunit;

namespace Journal.UnitTests;

public class GeneralTests
{
    [Fact]
    public void Int64_CanEncodeAndDecodeProperly()
    {
        long expected = 1234;
        var data = expected.ToBytes();
        var actual = data.ToInt64();

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Int64_ArrayTooSmallThrowsArgumentException()
    {
        var smallArray = new byte[4];
        Assert.Throws<ArgumentException>(() => 1234L.ToBytes(smallArray));
        Assert.Throws<ArgumentException>(() => smallArray.ToInt64());
    }


    [Fact]
    public void Int32_CanEncodeAndDecodeProperly()
    {
        int expected = 1234;
        var data = expected.ToBytes();
        var actual = data.ToInt32();

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Int32_ArrayTooSmallThrowsArgumentException()
    {
        var smallArray = new byte[2];
        Assert.Throws<ArgumentException>(() => 1234.ToBytes(smallArray));
        Assert.Throws<ArgumentException>(() => smallArray.ToInt32());
    }
}