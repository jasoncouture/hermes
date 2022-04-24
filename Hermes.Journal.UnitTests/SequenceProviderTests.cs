using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Hermes.Journal.UnitTests;

public class SequenceProviderTests
{
    [Fact]
    public async Task SequenceProviderIsThreadSafe()
    {
        ISequenceProvider sequenceProvider = new SequenceProvider();
        ConcurrentBag<long> sequenceNumbers = new ConcurrentBag<long>();
        const int SequencePerThread = 1000;
        const int Threads = 32;
        const int ExpectedCount = SequencePerThread * Threads;

        void CollectSequenceNumber()
        {
            for (var x = 0; x < 1000; x++)
            {
                sequenceNumbers.Add(sequenceProvider.GetNextSequence());
            }
        }

        var tasks = Enumerable.Range(0, 32).Select(i => Task.Run(CollectSequenceNumber)).ToArray();
        await Task.WhenAll(tasks);


        Assert.Equal(sequenceNumbers.Count, ExpectedCount);
        var sortedValues = sequenceNumbers.Distinct().OrderBy(i => i).Select((i, index) => (i, (long) index)).ToArray();
        Assert.Equal(sortedValues.Length, ExpectedCount);
        sortedValues.Aggregate(-1L, (previous, tuple) =>
        {
            Assert.Equal(previous, tuple.i - 1);
            return tuple.i;
        });
    }

    [Fact]
    public void SequenceResetGivesResetValueAsNextSequenceNumber()
    {
        const long ExpectedValue = 100L;
        ISequenceProvider sequenceProvider = new SequenceProvider();
        sequenceProvider.ResetTo(ExpectedValue);
        var actualValue = sequenceProvider.GetNextSequence();
        Assert.Equal(ExpectedValue, actualValue);
    }

    [Fact]
    public void SequenceProviderConstructorReturnsStartValueAsFirstValue()
    {
        const long ExpectedValue = 100L;
        ISequenceProvider sequenceProvider = new SequenceProvider(ExpectedValue);
        var actualValue = sequenceProvider.GetNextSequence();

        Assert.Equal(ExpectedValue, actualValue);
    }

    [Fact]
    public void CurrentPropertyReturnsLastValue()
    {
        ISequenceProvider sequenceProvider = new SequenceProvider();
        var expectedValue = sequenceProvider.GetNextSequence();
        Assert.Equal(expectedValue, sequenceProvider.Current);
    }
}