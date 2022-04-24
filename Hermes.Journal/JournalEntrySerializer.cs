using System.Diagnostics;
using System.Text;

namespace Hermes.Journal;

public class JournalEntrySerializer : ISerializer<JournalEntry>
{
    public IBorrowedArray<byte> Serialize(JournalEntry journalEntry)
    {
        var size = ComputeSize(journalEntry);
        var buffer = BorrowedArray<byte>.Rent(size);
        using var memoryStream = new MemoryStream(buffer.Array, buffer.Offset, buffer.Count, true);
        using var binaryWriter = new BinaryWriter(memoryStream, Encoding.UTF8);
        binaryWriter.Write(size);
        binaryWriter.Write((byte) 0);
        binaryWriter.Write(journalEntry.SequenceNumber);
        binaryWriter.Write(journalEntry.Partition);
        binaryWriter.Write(journalEntry.JournalEntryHeaders.Count());
        foreach (var entry in journalEntry.JournalEntryHeaders)
        {
            WriteString(binaryWriter, entry.Key);
            binaryWriter.Write(entry.Values.Count());
            foreach (var value in entry.Values)
            {
                WriteString(binaryWriter, value);
            }
        }

        WriteNullableByteArray(binaryWriter, journalEntry.Key);
        WriteNullableByteArray(binaryWriter, journalEntry.Data);

        return buffer;
    }

    private void WriteNullableByteArray(BinaryWriter writer, byte[]? bytes)
    {
        var length = bytes == null ? -1 : bytes.Length;
        writer.Write(length);
        if (length <= 0) return;
        writer.Write(bytes!);
    }

    private void WriteString(BinaryWriter writer, string value)
    {
        var byteCount = Encoding.UTF8.GetByteCount(value);
        writer.Write(byteCount);
        using var buffer = BorrowedArray<byte>.Rent(byteCount);
        Encoding.UTF8.GetBytes(value, 0, value.Length, buffer.Array, buffer.Offset);
        writer.Write(buffer.Array, buffer.Offset, buffer.Count);
    }

    private string ReadString(BinaryReader reader)
    {
        var byteCount = reader.ReadInt32();
        return Encoding.UTF8.GetString(reader.ReadBytes(byteCount));
    }

    private int ComputeSize(JournalEntry journalEntry)
    {
        var headerSize = ComputeHeaderSize(journalEntry.JournalEntryHeaders);
        var messageSize = sizeof(int) + // Message Size
                          sizeof(byte) + // Message Version
                          sizeof(long) + // Sequence Number
                          sizeof(int) + // Partition
                          sizeof(int) + // Key Length
                          (journalEntry.Key?.Length ?? 0) + // Key
                          sizeof(int) + // Data Length
                          (journalEntry.Data?.Length ?? 0); // Data

        return headerSize + messageSize;
    }

    private int ComputeHeaderSize(IEnumerable<JournalEntryHeader> headers)
    {
        int size = 4; // Header count
        foreach (var header in headers)
        {
            size += sizeof(int); // Header key length
            size += Encoding.UTF8.GetByteCount(header.Key);
            size += sizeof(int); // Header value count
            foreach (var headerValue in header.Values)
            {
                size += sizeof(int); // Header value length
                size += Encoding.UTF8.GetByteCount(headerValue);
            }
        }

        return size;
    }

    public JournalEntry Deserialize(ArraySegment<byte> bytes)
    {
        if (!TryDeserialize(bytes, out var journalEntry, out var versionMismatch))
        {
            if (versionMismatch) throw new InvalidOperationException("Message version is not valid");
            throw new InvalidOperationException("Failed to deserialize journal entry");
        }

        return journalEntry!;
    }

    private byte[]? ReadNullableByteArray(BinaryReader reader)
    {
        var length = reader.ReadInt32();
        if (length == 0) return Array.Empty<byte>();
        if (length == -1) return null;
        return reader.ReadBytes(length);
    }

    private JournalEntryHeader[] ReadHeaders(BinaryReader binaryReader)
    {
        var headerCount = binaryReader.ReadInt32();
        var headers = new JournalEntryHeader[headerCount];
        for (var x = 0; x < headerCount; x++)
        {
            var key = ReadString(binaryReader);
            var valueCount = binaryReader.ReadInt32();
            var values = new string[valueCount];
            for (var y = 0; y < valueCount; y++)
            {
                values[y] = ReadString(binaryReader);
            }

            headers[x] = new JournalEntryHeader(key, values);
        }

        return headers;
    }

    public bool TryDeserialize(ArraySegment<byte> bytes, out JournalEntry? result)
    {
        return TryDeserialize(bytes, out result, out _);
    }

    private bool TryDeserialize(ArraySegment<byte> bytes, out JournalEntry? result, out bool invalidVersion)
    {
        invalidVersion = false;
        result = null;
        if (bytes.Array == null) return false;
        Debug.Assert(bytes.Array != null, "bytes.Array != null");
        if (bytes.Count < 4) return false;
        using var memoryStream = new MemoryStream(bytes.Array, bytes.Offset, bytes.Count, false);
        using var binaryReader = new BinaryReader(memoryStream, Encoding.UTF8);
        var totalSize = binaryReader.ReadInt32();
        if (bytes.Count < totalSize)
        {
            return false;
        }

        if (binaryReader.ReadByte() != 0)
        {
            invalidVersion = true;
            return false;
        }

        var sequence = binaryReader.ReadInt64();
        var partition = binaryReader.ReadInt32();
        JournalEntryHeader[] headers = ReadHeaders(binaryReader);

        var key = ReadNullableByteArray(binaryReader);
        var data = ReadNullableByteArray(binaryReader);

        result = new JournalEntry(sequence, partition, key, data, headers);
        return true;
    }
}