using System.Buffers;
using System.Collections;

namespace Hermes.Journal;

public class BorrowedArray<T> : IBorrowedArray<T>
{
    private volatile int _disposed = 0;
    private readonly T[] _array;
    private readonly ArrayPool<T>? _pool;
    private int _count;
    private int _offset;

    private BorrowedArray(T[] array, ArrayPool<T>? pool, int size)
    {
        _array = array;
        _pool = pool;
        Offset = 0;
        Count = size;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed == 1) throw new ObjectDisposedException(nameof(BorrowedArray<T>));
    }

    public static BorrowedArray<T> Rent(int size, ArrayPool<T> pool)
    {
        if (size == 0) return Empty;
        var array = pool.Rent(size);
        return new BorrowedArray<T>(array, pool, size);
    }

    public static BorrowedArray<T> Empty { get; } = new(System.Array.Empty<T>(), null, 0);

    public static BorrowedArray<T> Rent(int size) => Rent(size, ArrayPool<T>.Shared);

    public void Dispose()
    {
        Dispose(true);
    }

    ~BorrowedArray()
    {
        Dispose(false);
    }
    
    private void Dispose(bool disposing)
    {
        if (_pool == null) return;
        var disposed = Interlocked.CompareExchange(ref _disposed, 1, 0);

        if (disposed == 1) return;
        if (!disposing) return; // Don't return to the pool if this object was orphaned.


        _pool?.Return(_array);
    }

    public T[] Array
    {
        get
        {
            ThrowIfDisposed();
            return _array;
        }
    }

    public ArraySegment<T> ArraySegment
    {
        get
        {
            ThrowIfDisposed();
            if (Count == 0) return ArraySegment<T>.Empty;
            return (ArraySegment<T>) this;
        }
    }

    public int Offset
    {
        get
        {
            ThrowIfDisposed();
            return _offset;
        }
        set
        {
            ThrowIfDisposed();
            if (value < 0) throw new InvalidOperationException("Value must be greater than or equal to 0");
            if (Capacity > 0 && value >= Capacity)
                throw new InvalidOperationException("Offset is larger than the available capacity");
            if (Capacity == 0 && value != 0)
                throw new InvalidOperationException("Offset is larger than the available capacity");

            if (value + Count > Capacity)
            {
                Count = Capacity - value;
            }

            _offset = value;
        }
    }

    public int Count
    {
        get
        {
            ThrowIfDisposed();
            return _count;
        }
        set
        {
            ThrowIfDisposed();
            if (value < 0) throw new InvalidOperationException("Value must be greater than or equal to 0");
            if (value > (Capacity - Offset))
                throw new InvalidOperationException("Count is larger than the available capacity");
            _count = value;
        }
    }

    public int Capacity => Array.Length;

    // These operators are explicit because they can throw if disposed.
    public static explicit operator T[](BorrowedArray<T> borrowedArray) => borrowedArray.Array;

    public static explicit operator ArraySegment<T>(BorrowedArray<T> borrowedArray) =>
        new(borrowedArray.Array, borrowedArray.Offset, borrowedArray.Count);

    private IEnumerable<T> Enumerate()
    {
        var offset = Offset;
        var count = Count;
        if (count == 0) yield break;
        for (var x = 0; x < count; x++)
        {
            yield return Array[offset + x];
        }
    }

    IEnumerator<T> IEnumerable<T>.GetEnumerator()
    {
        return Enumerate().GetEnumerator();
    }

    public IEnumerator GetEnumerator()
    {
        return Enumerate().GetEnumerator();
    }
}