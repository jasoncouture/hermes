using System.Runtime.CompilerServices;

namespace Journal.Journal;

public interface IJournal
{
    ValueTask<long> WriteEntryAsync(ArraySegment<byte> data, CancellationToken cancellationToken);

    ValueTask<long> WriteEntryAsync(ArraySegment<byte> data, long virtualAddress,
        CancellationToken cancellationToken);

    ValueTask WaitForCommit(CancellationToken cancellationToken);

    IAsyncEnumerable<JournalData> ScanJournalAsync(long startOffset = -1, long endOffset = -1,
        [EnumeratorCancellation] CancellationToken cancellationToken = default);
}