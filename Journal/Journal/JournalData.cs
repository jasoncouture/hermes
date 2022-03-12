using Journal.Encoding;

namespace Journal.Journal;

public record JournalData(long Offset, ArraySegment<byte> Data)
{
    public int Size => sizeof(int) + sizeof(long) + Data.Count;
    public void WriteTo(ArraySegment<byte> segment)
    {
        if (segment.Count < Size) throw new ArgumentException("Array segment is too small to write this data.");
        Size.ToBytes(segment);
        segment = segment.Slice(sizeof(int));
        Offset.ToBytes(segment);
        segment = segment.Slice(sizeof(long));
        Data.CopyTo(segment);
    }

    public static JournalData ReadFrom(long fileOffset, ReadOnlyMemory<byte> data)
    {
        var expectedLength = data.Slice(0, sizeof(int)).ToInt32();
        data = data.Slice(sizeof(int));
        var offset = data.Slice(0, sizeof(long)).ToInt64();
        data = data.Slice(sizeof(long));
        
        data = data.Slice(0, expectedLength - sizeof(int) - sizeof(long));
        if (offset <= 0) offset = fileOffset;
        return new JournalData(offset, data.ToArray());
    }
}