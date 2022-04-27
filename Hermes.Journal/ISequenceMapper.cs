namespace Hermes.Journal;

public interface ISequenceMapper
{
    void SetPhysicalOffset(long sequence, long physicalOffset);
    long GetPhysicalOffset(long sequence);
    void RemovePhysicalOffset(long sequence);
    void RemoveOffsetsBefore(long firstSequence);

    int Count { get; }
    IEnumerable<long> GetSequences();
}