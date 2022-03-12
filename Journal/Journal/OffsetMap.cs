using System.Collections.Concurrent;
using Journal.Encoding;

namespace Journal.Journal;

public class OffsetMap
{
    private ConcurrentDictionary<long, long> _forwardOffsetMapStorage = new ();
    private ConcurrentDictionary<long, long> _reverseOffsetMapStorage = new ();
    public int Count => _forwardOffsetMapStorage.Count;
    public void Map(long fileOffset, long virtualOffset)
    {
        if (fileOffset == virtualOffset) return;
        _forwardOffsetMapStorage.TryAdd(virtualOffset, fileOffset);
        _reverseOffsetMapStorage.TryAdd(fileOffset, virtualOffset);
    }

    public byte[] GetSnapshot()
    {
        return EnumerateSnapshot().ToArray();
    }
    private IEnumerable<byte> EnumerateSnapshot()
    {
        foreach (var (physicalAddress, virtualAddress) in GetAllOffsets())
        {
            foreach (var addressByte in virtualAddress.ToBytes())
                yield return addressByte;
            foreach (var addressByte in physicalAddress.ToBytes())
                yield return addressByte;
        }
    }

    public IEnumerable<(long FileOffset, long VirtualOffset)> GetAllOffsets()
    {
        foreach (var key in _forwardOffsetMapStorage.Keys.ToArray())
        {
            var virtualAddress = key;
            var physicalAddress = GetFileOffset(virtualAddress);
            if (virtualAddress == physicalAddress) continue;
            yield return (physicalAddress, virtualAddress);
        }
    }

    public static OffsetMap FromSnapshot(ArraySegment<byte> snapshot)
    {
        var offsetMap = new OffsetMap();
        
        while (snapshot.Count >= 16)
        {
            var virtualAddress = snapshot.ToInt64();
            snapshot = snapshot.Slice(8);
            var physicalAddress = snapshot.ToInt64();
            snapshot = snapshot.Slice(8);
            offsetMap.Map(physicalAddress, virtualAddress);
        }

        return offsetMap;
    }

    public long GetFileOffset(long virtualOffset)
    {
        return _forwardOffsetMapStorage.GetValueOrDefault(virtualOffset, virtualOffset);
    }

    public long GetVirtualOffset(long fileOffset)
    {
        return _reverseOffsetMapStorage.GetValueOrDefault(fileOffset, fileOffset);
    }
}