namespace Journal.Encoding;

public static class ByteEncoding
{
    public static void ToBytes(this int value, ArraySegment<byte> segment)
    {
        if (segment.Count < 4) throw new ArgumentException("Array segment to write bytes to must be 4 bytes or more");
        for (var x = 0; x < 4; x++)
        {
            var shiftBy = x * 8;
            var currentByte = (byte) (((uint) value >> shiftBy) & 0xFF);
            segment[3 - x] = currentByte;
        }
    }

    public static byte[] ToBytes(this int value)
    {
        var returnValue = new byte[sizeof(int)];
        value.ToBytes(returnValue);
        return returnValue;
    }

    public static void ToBytes(this long value, ArraySegment<byte> segment)
    {
        if (segment.Count < 8) throw new ArgumentException("Array segment to write bytes to must be 8 bytes or more");
        for (var x = 0; x < 8; x++)
        {
            var shiftBy = x * 8;
            var currentByte = (byte) (((ulong) value >> shiftBy) & 0xFF);
            segment[7 - x] = currentByte;
        }
    }

    public static byte[] ToBytes(this long value)
    {
        var returnValue = new byte[sizeof(long)];
        value.ToBytes(returnValue);
        return returnValue;
    }

    public static int ToInt32(this byte[] array) => ToInt32((ArraySegment<byte>) array);

    public static int ToInt32(this ArraySegment<byte> segment) => ToInt32((ReadOnlyMemory<byte>) segment);

    public static int ToInt32(this ReadOnlyMemory<byte> segment)
    {
        if (segment.Length < 4) throw new ArgumentException("Array segment must be >= 4 bytes");
        int returnValue = 0;
        for (var x = 0; x < 4; x++)
        {
            returnValue <<= 8;
            returnValue |= segment.Span[x];
        }

        return returnValue;
    }

    public static long ToInt64(this byte[] array) => ToInt64((ReadOnlyMemory<byte>) array);
    public static long ToInt64(this ArraySegment<byte> segment) => ToInt64((ReadOnlyMemory<byte>) segment);
    public static long ToInt64(this ReadOnlyMemory<byte> segment)
    {
        if (segment.Length < 8) throw new ArgumentException("Array segment must be >= 8 bytes");
        long returnValue = 0;
        for (var x = 0; x < 8; x++)
        {
            returnValue <<= 8;
            returnValue |= segment.Span[x];
        }

        return returnValue;
    }
}