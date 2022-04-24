using System;
using System.Linq;
using System.Text;
using Xunit;

namespace Hermes.Journal.UnitTests;

public class PartitionSelectorTests
{
    [Fact]
    public void PartitionSelectorReturnsSamePartitionForSameKey()
    {
        var input = Guid.NewGuid().ToByteArray();
        var partitionSelector = new PartitionSelector();

        var result1 = partitionSelector.SelectPartitionForKey(input);
        var result2 = partitionSelector.SelectPartitionForKey(input);

        Assert.Equal(result1, result2);
    }

    [Fact]
    public void PartitionSelectorSelectsPartitionDependentOnKey()
    {
        var input1 = "TheQuickBrownFox";
        var input2 = "SomeOtherKey";
        var partitionSelector = new PartitionSelector();

        var result1 = partitionSelector.SelectPartitionForKey(Encoding.UTF8.GetBytes(input1));
        var result2 = partitionSelector.SelectPartitionForKey(Encoding.UTF8.GetBytes(input2));

        Assert.NotEqual(result1, result2);
    }

    [Fact]
    public void RandomPartitionSelectsDifferentPartitionAtLeastOneOutOfFiveTimes()
    {
        var partitionSelector = new PartitionSelector();
        int[] partitions = new int[5];

        for (var x = 0; x < partitions.Length; x++)
        {
            partitions[x] = partitionSelector.SelectRandomPartition();
        }

        // If the 3 values are identical, this will return an empty result.
        Assert.NotEmpty(partitions.Distinct().Skip(1));
    }
}