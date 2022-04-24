namespace Hermes.Journal;

public interface IBorrowedArray<T> : IDisposable, IEnumerable<T>
{
    T[] Array { get; }
    ArraySegment<T> ArraySegment { get; }
    int Offset { get; set; }
    int Count { get; set; }
    int Capacity { get; }
}