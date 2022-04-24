using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;

namespace Hermes.Journal;

public class Sha256HashAlgorithm : IHashAlgorithm
{
    public byte[] ComputeHash(ArraySegment<byte> data)
    {
        using var hasher = SHA256.Create();
        return hasher.ComputeHash(data.Array ?? throw new InvalidOperationException("Array must not be null"),
            data.Offset, data.Count);
    }
}