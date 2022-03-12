using System.Collections.Immutable;
using Journal.Encoding;

namespace Journal.Journal.Remote.Protocol;

public record RemoteJournalResponse(long StreamId, IEnumerable<ReadOnlyMemory<byte>> Segments)
{
    public RemoteJournalResponse(long streamId) : this(streamId, Enumerable.Empty<ReadOnlyMemory<byte>>())
    {
    }

    public RemoteJournalResponse(long streamId, params ReadOnlyMemory<byte>[] segments) : this(streamId,
        segments.ToImmutableArray())
    {
    }

    private const int HeaderSize = sizeof(int) + sizeof(long) + sizeof(int);
    public int Size => HeaderSize + (sizeof(int) * Count) + DataSize;
    public int Count => Segments.Count();
    public int DataSize => Segments.Sum(i => i.Length);

    public void WriteTo(ArraySegment<byte> segment)
    {
        (~Size).ToBytes(segment);
        segment = segment.Slice(sizeof(int));
        StreamId.ToBytes(segment);
        segment = segment.Slice(sizeof(long));
        Count.ToBytes(segment);
        segment = segment.Slice(sizeof(int));
        foreach (var dataSegment in Segments)
        {
            dataSegment.Length.ToBytes(segment);
            segment = segment.Slice(sizeof(int));
            dataSegment.CopyTo(segment);
            segment = segment.Slice(dataSegment.Length);
        }
    }

    public static bool IsCompletePacket(ReadOnlyMemory<byte> bytes, out int sizeNeeded)
    {
        sizeNeeded = 4;
        if (bytes.Length < sizeof(int)) return false;
        sizeNeeded = ~bytes.ToInt32();
        if (sizeNeeded <= 0)
        {
            sizeNeeded = 0;
            return false;
        }
        if (bytes.Length >= sizeNeeded) return true;
        return false;
    }

    public static bool TryReadFrom(ReadOnlyMemory<byte> bytes, out RemoteJournalResponse command)
    {
        command = default;
        if (!IsCompletePacket(bytes, out var size)) return false;
        command = ReadFrom(bytes.Slice(0, size));
        return true;
    }

    public static RemoteJournalResponse ReadFrom(ReadOnlyMemory<byte> segment)
    {
        var size = ~segment.ToInt32();
        segment = segment.Slice(sizeof(int));
        var streamId = segment.ToInt64();
        segment = segment.Slice(sizeof(long));
        var innerSegmentCount = segment.ToInt32();
        segment = segment.Slice(sizeof(int));
        var segments = new List<ReadOnlyMemory<byte>>();
        size -= HeaderSize;
        for (var x = 0; x < innerSegmentCount; x++)
        {
            var segmentLength = segment.ToInt32();
            segment = segment.Slice(sizeof(int));
            if (size - segmentLength < 0) throw new InvalidDataException();
            var innerSegment = new byte[segmentLength];
            var innerReadOnlyMemory = segment.Slice(0, segmentLength);
            innerReadOnlyMemory.CopyTo(innerSegment);
            segments.Add(innerSegment);
            segment = segment.Slice(segmentLength);
        }

        if (segments.Count != innerSegmentCount) throw new InvalidDataException();
        return new RemoteJournalResponse(streamId, segments);
    }
}