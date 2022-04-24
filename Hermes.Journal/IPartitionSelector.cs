namespace Hermes.Journal;

public interface IPartitionSelector
{
    int SelectRandomPartition();
    int SelectPartitionForKey(ArraySegment<byte> key);
}