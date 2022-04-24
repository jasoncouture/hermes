namespace Hermes.Journal;

public interface ISequenceProvider
{
    public void ResetTo(long start);
    public long GetNextSequence();
    public long Current { get; }
}