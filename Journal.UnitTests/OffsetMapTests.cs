using System.Linq;
using Journal.Journal;
using Xunit;

namespace Journal.UnitTests;

public class OffsetMapTests
{
    [Fact]
    public void OffsetMap_CanReadAndWriteSnapshot()
    {
        int size = 256;
        var physicalOffsets = Enumerable.Range(0, size).Select(i => (long) i).ToArray();
        var virtualOffsets = Enumerable.Range(1, size).Select(i => (long) i).ToArray();
        var pairs = physicalOffsets.Zip(virtualOffsets);

        var expected = new OffsetMap();
        foreach (var pair in pairs)
        {
            expected.Map(pair.First, pair.Second);
        }

        var blob = expected.GetSnapshot();
        var actual = OffsetMap.FromSnapshot(blob);
        var originalOffsets = expected.GetAllOffsets();
        var decodedOffsets = actual.GetAllOffsets();
        var combinedOffsets = originalOffsets.OrderBy(i => i.FileOffset).Zip(decodedOffsets.OrderBy(i => i.FileOffset));
        int count = 0;
        foreach (var pair in combinedOffsets)
        {
            count++;
            Assert.Equal(pair.First.FileOffset, pair.Second.FileOffset);
            Assert.Equal(pair.First.VirtualOffset, pair.Second.VirtualOffset);
        }

        Assert.Equal(physicalOffsets.Length, count);
    }

    [Fact]
    public void OffsetMap_DoesNotStoreDirectAddresses()
    {
        var map = new OffsetMap();
        for (var x = 0; x < 100; x++)
        {
            map.Map(x, x);
        }

        var data = map.GetSnapshot();
        Assert.Empty(data);
    }

    [Fact]
    public void OffsetMap_StoresDataWhenAddressesAreVirtual()
    {
        var expectedSize = 16;
        var map = new OffsetMap();
        for (var x = 0; x < 100; x++)
        {
            map.Map(x, x);
        }

        map.Map(500, 101);
        var snapshot = map.GetSnapshot();
        Assert.NotEmpty(snapshot);
        Assert.Equal(expectedSize, snapshot.Length);
    }

    [Fact]
    public void OffsetMap_ReturnsCorrectAddress()
    {
        var map = new OffsetMap();
        map.Map(1, 2);
        Assert.Equal(1, map.GetFileOffset(2));
        Assert.Equal(2, map.GetVirtualOffset(1));
    }

    [Fact]
    public void OffsetMap_ReturnsInputWhenAddressNotFound()
    {
        var map = new OffsetMap();
        map.Map(1, 2);
        Assert.Equal(1, map.GetFileOffset(1));
        Assert.Equal(2, map.GetVirtualOffset(2));
    }
}