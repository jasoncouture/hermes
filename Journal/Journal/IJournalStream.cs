namespace Journal.Journal;

public interface IJournalStream : IDisposable
{
    IAsyncEnumerable<JournalData> ScanAsync(CancellationToken cancellationToken = default);
}