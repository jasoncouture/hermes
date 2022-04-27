using System.Collections.Concurrent;

namespace Hermes.Journal;

public sealed class SequenceMapper : ISequenceMapper
{
    private readonly ConcurrentDictionary<long, long> _sequenceMap = new();
    

    private static long ValidateSequence(long sequence) => sequence >= 0
        ? sequence
        : throw new ArgumentException("Sequence must be a positive number", nameof(sequence));

    private static long ValidatePhysicalOffset(long physicalOffset) => physicalOffset >= 0
        ? physicalOffset
        : throw new ArgumentException("Physical offset must be a positive number", nameof(physicalOffset));
    

    public void SetPhysicalOffset(long sequence, long physicalOffset)
    {
        ValidateSequence(sequence);
        ValidatePhysicalOffset(physicalOffset);
        var partitionMap = _sequenceMap;
        if (partitionMap.TryAdd(sequence, physicalOffset)) return;
        throw new InvalidOperationException("Duplicate key detected");
    }

    public long GetPhysicalOffset(long sequence)
    {
        var partitionMap = _sequenceMap;
        ValidateSequence(sequence);
        var offset = partitionMap.GetValueOrDefault(sequence, -1);
        if (offset < 0) throw new KeyNotFoundException();
        return offset;
    }

    public void RemoveOffsetsBefore(long firstSequence)
    {
        var partitionMap = _sequenceMap;
        ValidateSequence(firstSequence);
        var keys = partitionMap.Keys.Where(i => i < firstSequence);
        foreach (var key in keys)
        {
            partitionMap.TryRemove(key, out _);
        }
    }

    public int Count => _sequenceMap.Count;

    public IEnumerable<long> GetSequences()
    {
        var partitionMap = _sequenceMap;
        return partitionMap.Keys;
    }

    public void RemovePhysicalOffset(long sequence)
    {
        var partitionMap = _sequenceMap;
        partitionMap.TryRemove(sequence, out _);
    }
}