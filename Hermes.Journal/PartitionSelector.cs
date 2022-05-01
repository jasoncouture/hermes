using FASTER.core;
using Force.Crc32;

namespace Hermes.Journal;

public sealed class PartitionSelector : IPartitionSelector
{
    private const int PartitionCount = 1024;
    private static int _nextPartition = 0;

    private static int GetNextPartition()
    {
        return Interlocked.Increment(ref _nextPartition) % PartitionCount;
    }

    public int SelectRandomPartition() => GetNextPartition();

    public int SelectPartitionForKey(ArraySegment<byte> key)
    {
        return ComputePartition(key, PartitionCount);
    }

    private static int ComputePartition(ArraySegment<byte> data, int partitionCount)
    {
        var partition = ((int) Crc32Algorithm.Compute(data.Array, data.Offset, data.Count) & 0x7FFFFFFF) %
                        partitionCount;
        return partition;
    }
}