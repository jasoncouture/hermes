using System.Text;
using Microsoft.Extensions.ObjectPool;

namespace Hermes.Journal;

public class StringHasher : IStringHasher
{
    private static ObjectPool<StringBuilder> _stringBuilderPool;

    static StringHasher()
    {
        _stringBuilderPool = new DefaultObjectPool<StringBuilder>(new DefaultPooledObjectPolicy<StringBuilder>(), 32);
        for (var x = 0; x < 256; x++)
        {
            hashValues[x] = x.ToString("x2");
        }
    }

    private readonly IHashAlgorithm _hashAlgorithm;
    private static string[] hashValues = new string[256];

    public StringHasher(IHashAlgorithm hashAlgorithm)
    {
        _hashAlgorithm = hashAlgorithm;
    }

    public string ComputeHash(ArraySegment<byte> data)
    {
        var hashedBytes = _hashAlgorithm.ComputeHash(data);
        return ToStringHash(hashedBytes);
    }

    private static string ToStringHash(byte[] data)
    {
        var builder = _stringBuilderPool.Get();
        builder.Clear();
        foreach (var @byte in data) builder.Append(hashValues[@byte]);
        var returnValue = builder.ToString();
        _stringBuilderPool.Return(builder);
        return returnValue;
    }
}