namespace Hermes.Journal;

public interface ISerializer<T>
{
    IBorrowedArray<byte> Serialize(T @object);
    T Deserialize(ArraySegment<byte> bytes);
    bool TryDeserialize(ArraySegment<byte> bytes, out T result);
}