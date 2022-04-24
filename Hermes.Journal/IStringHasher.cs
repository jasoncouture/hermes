namespace Hermes.Journal;

public interface IStringHasher
{
    string ComputeHash(ArraySegment<byte> data);
}