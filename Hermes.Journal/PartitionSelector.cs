using Force.Crc32;

namespace Hermes.Journal;

public class PartitionSelector : IPartitionSelector
{
    private const int PartitionCount = 1024;

    public int SelectRandomPartition() => ComputePartition(Guid.NewGuid().ToByteArray(), PartitionCount);

    public int SelectPartitionForKey(ArraySegment<byte> key)
    {
        return ComputePartition(key, PartitionCount);
    }

    private static int ComputePartition(ArraySegment<byte> data, int partitionCount)
    {
        var partition = ((int) Crc32Algorithm.Compute(data.Array, data.Offset, data.Count) & 0x7FFFFFFF) %
                        PartitionCount;
        return partition;
    }
}