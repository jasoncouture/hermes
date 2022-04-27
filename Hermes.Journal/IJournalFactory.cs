namespace Hermes.Journal;

public interface IJournalFactory
{
    ValueTask<IJournal> GetOrCreateJournalAsync(string topic, int partition, CancellationToken cancellationToken);
}