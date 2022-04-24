namespace Hermes.Journal;

public interface IHashAlgorithm
{
    byte[] ComputeHash(ArraySegment<byte> data);
}