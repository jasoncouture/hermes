using System;
using Xunit;

namespace Hermes.Journal.UnitTests;

public class BorrowedArrayTests
{
    [Fact]
    public void BorrowedArrayThrowsInvalidOperationExceptionWhenCountGreaterThanCapacity()
    {
        using var borrowedArray = BorrowedArray<byte>.Rent(100);
        Assert.Throws<InvalidOperationException>(() => borrowedArray.Count = borrowedArray.Capacity + 1);
    }

    [Fact]
    public void BorrowedArrayThrowsInvalidOperationExceptionForNegativeCount()
    {
        using var borrowedArray = BorrowedArray<byte>.Rent(100);
        Assert.Throws<InvalidOperationException>(() => borrowedArray.Count = -1);
    }

    [Fact]
    public void BorrowedArrayThrowsInvalidOperationExceptionWhenOffsetGreaterThanCapacity()
    {
        using var borrowedArray = BorrowedArray<byte>.Rent(100);
        Assert.Throws<InvalidOperationException>(() => borrowedArray.Offset = borrowedArray.Capacity + 1);
    }

    [Fact]
    public void BorrowedArrayThrowsInvalidOperationExceptionForNegativeOffset()
    {
        using var borrowedArray = BorrowedArray<byte>.Rent(100);
        Assert.Throws<InvalidOperationException>(() => borrowedArray.Offset = -1);
    }

    [Fact]
    public void BorrwoedArrayThrowsInvalidOperationExceptionWhenCountExceedsCapacityIncludingOffset()
    {
        using var borrowedArray = BorrowedArray<byte>.Rent(100);
        borrowedArray.Offset = borrowedArray.Capacity - 2;

        borrowedArray.Count = 2;
        Assert.Equal(2, borrowedArray.Count);
        Assert.Throws<InvalidOperationException>(() => borrowedArray.Count = 3);
    }

    [Fact]
    public void BorrowedArrayHasInitialOffsetOfZero()
    {
        using var borrowedArray = BorrowedArray<byte>.Rent(100);
        Assert.Equal(0, borrowedArray.Offset);
    }


    [Fact]
    public void BorrowedArrayHasInitialCountMatchingRequestedSize()
    {
        EnsureSize100ExistsInPool();
        // This _should_ return a 100 byte array.
        // But the pool may decide to create a new array, so don't rely on capacity for test.
        using var borrowedArray = BorrowedArray<byte>.Rent(50);

        Assert.Equal(50, borrowedArray.Count);
    }

    [Fact]
    public void BorrowedArrayCanBeCastToArraySegment()
    {
        EnsureSize100ExistsInPool();
        using var borrowedArray = BorrowedArray<byte>.Rent(50);
        ArraySegment<byte> segment = (ArraySegment<byte>) borrowedArray;
        Assert.Equal(borrowedArray.Offset, segment.Offset);
        Assert.Equal(borrowedArray.Count, segment.Count);
        Assert.Same(borrowedArray.Array, segment.Array);
    }

    [Fact]
    public void BorrowedArrayCanBeCastToArray()
    {
        EnsureSize100ExistsInPool();
        using var borrowedArray = BorrowedArray<byte>.Rent(50);
        var array = (byte[]) borrowedArray;
        Assert.Equal(borrowedArray.Capacity, array.Length);
        Assert.Same(borrowedArray.Array, array);
    }

    [Fact]
    public void BorrowedArrayThrowsObjectDisposedExceptionWhenDisposed()
    {
        var disposedBorrowedArray = BorrowedArray<byte>.Rent(100);
        disposedBorrowedArray.Dispose();

        Assert.Throws<ObjectDisposedException>(() => disposedBorrowedArray.Array);
        Assert.Throws<ObjectDisposedException>(() => disposedBorrowedArray.ArraySegment);
        Assert.Throws<ObjectDisposedException>(() => disposedBorrowedArray.Offset);
        Assert.Throws<ObjectDisposedException>(() => disposedBorrowedArray.Count);
        Assert.Throws<ObjectDisposedException>(() => disposedBorrowedArray.Capacity);
        Assert.Throws<ObjectDisposedException>(() => (ArraySegment<byte>) disposedBorrowedArray);
        Assert.Throws<ObjectDisposedException>(() => (byte[]) disposedBorrowedArray);
    }

    [Fact]
    public void EmptyBorrowedArrayIgnoresDispose()
    {
        var borrowedArray = BorrowedArray<byte>.Empty;
        borrowedArray.Dispose();
        Assert.Same(Array.Empty<byte>(), borrowedArray.Array);
        Assert.Same(BorrowedArray<byte>.Empty, borrowedArray);
    }

    // Request a 100 byte array from the pool, and immediately return it.
    // This ensures that a 100 byte array is available in the pool.
    private void EnsureSize100ExistsInPool()
    {
        using var borrowedArray = BorrowedArray<byte>.Rent(100);
        borrowedArray.Dispose();
    }
}