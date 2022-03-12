using System.Collections.Immutable;
using Journal.Encoding;

namespace Journal.Journal.Remote.Protocol;

public record RemoteJournalCommand(long StreamId, RemoteJournalCommandType Type,
    IEnumerable<ArraySegment<byte>> Segments)
{
    public RemoteJournalCommand(long streamId, RemoteJournalCommandType type) : this(streamId, type,
        Enumerable.Empty<ArraySegment<byte>>())
    {
    }

    public RemoteJournalCommand(long streamId, RemoteJournalCommandType type, params ArraySegment<byte>[] segments) :
        this(streamId, type, segments.ToImmutableArray())
    {
    }

    //                             TotalSize      StreamId       Type          Count     
    private const int HeaderSize = sizeof(int) + sizeof(long) + sizeof(int) + sizeof(int);

    //                              (Repeat: Length, Segment)
    public int Size => HeaderSize + (sizeof(int) * Count) + DataSize;
    public int Count => Segments.Count();
    public int DataSize => Segments.Sum(i => i.Count);

    public void WriteTo(ArraySegment<byte> segment)
    {
        Size.ToBytes(segment);
        segment = segment.Slice(sizeof(int));
        StreamId.ToBytes(segment);
        segment = segment.Slice(sizeof(long));
        ((int) Type).ToBytes(segment);
        segment = segment.Slice(sizeof(int));
        Count.ToBytes(segment);
        segment = segment.Slice(sizeof(int));
        foreach (var dataSegment in Segments)
        {
            dataSegment.Count.ToBytes(segment);
            segment = segment.Slice(sizeof(int));
            dataSegment.CopyTo(segment);
            segment = segment.Slice(dataSegment.Count);
        }
    }

    public static bool IsCompletePacket(ReadOnlyMemory<byte> bytes, out int sizeNeeded)
    {
        sizeNeeded = 4;
        if (bytes.Length < sizeof(int)) return false;
        sizeNeeded = bytes.ToInt32();
        if (sizeNeeded <= 0)
        {
            sizeNeeded = 0;
            return false;
        }
        if (bytes.Length >= sizeNeeded) return true;
        return false;
    }

    public static bool TryReadFrom(ReadOnlyMemory<byte> bytes, out RemoteJournalCommand command)
    {
        command = default;
        if (!IsCompletePacket(bytes, out var size)) return false;
        command = ReadFrom(bytes.Slice(0, size));
        return true;
    }

    public static RemoteJournalCommand ReadFrom(ReadOnlyMemory<byte> segment)
    {
        var size = segment.ToInt32();
        if (size < 0) throw new InvalidOperationException("Response packet detected.");
        segment = segment.Slice(sizeof(int));
        var streamId = segment.ToInt64();
        segment = segment.Slice(sizeof(long));
        var commandType = (RemoteJournalCommandType) segment.ToInt32();
        segment = segment.Slice(sizeof(int));
        var innerSegmentCount = segment.ToInt32();
        segment = segment.Slice(sizeof(int));
        List<ArraySegment<byte>> segments = new List<ArraySegment<byte>>();
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
        return new RemoteJournalCommand(streamId, commandType, segments);
    }
}