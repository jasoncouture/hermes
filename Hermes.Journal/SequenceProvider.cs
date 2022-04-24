namespace Hermes.Journal;

public class SequenceProvider : ISequenceProvider
{
    private const long DefaultStartSequenceNumber = 0;
    private long _current = DefaultStartSequenceNumber - 1;

    public SequenceProvider()
    {
    }

    public SequenceProvider(long sequenceStart) : this()
    {
        ResetTo(sequenceStart);
    }

    public void ResetTo(long start)
    {
        Interlocked.Exchange(ref _current, start - 1);
    }

    public long Current => Interlocked.Read(ref _current);

    public long GetNextSequence() => Interlocked.Increment(ref _current);
}