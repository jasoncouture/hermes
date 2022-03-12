namespace Journal.Journal.Remote;

public class MirrorMaker
{
    private readonly IJournal _remoteJournal;
    private readonly IJournal _localJournal;

    public MirrorMaker(IJournal remoteJournal, IJournal localJournal)
    {
        _remoteJournal = remoteJournal;
        _localJournal = localJournal;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        long lastKnownVirtualOffset = 0;
        // Scan the existing journal
        await foreach (var entry in _localJournal.ScanJournalAsync(endOffset: 0, cancellationToken: cancellationToken))
        {
            lastKnownVirtualOffset = entry.Offset;
        }

        // Subscribe to the remote journal, and skip entries we have, adding new entries as they arrive.
        await foreach (var journalData in _remoteJournal.ScanJournalAsync(cancellationToken: cancellationToken))
        {
            if (journalData.Offset <= lastKnownVirtualOffset) continue;
            await _localJournal.WriteEntryAsync(journalData.Data, journalData.Offset, cancellationToken);
        }
    }
}