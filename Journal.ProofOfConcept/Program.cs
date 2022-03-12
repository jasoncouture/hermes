// See https://aka.ms/new-console-template for more information

using Journal;
using Journal.Background;

var manager = new PeriodTaskManager();
var service = new PeriodicTaskRunner(manager, manager);
await service.StartAsync(CancellationToken.None);

var journal = new Journal.Journal.LocalJournal(Path.Combine("partition", "0"), manager);
var offset = await journal.WriteEntryAsync(new ArraySegment<byte>(new byte[1]), CancellationToken.None);
await journal.WaitForCommit(CancellationToken.None);
Console.WriteLine("Wrote offset {0}", offset);
await foreach (var entry in journal.ScanJournalAsync(startOffset: 0, endOffset: offset))
{
    Console.WriteLine("{0} byte(s) read at offset {1}", entry.Data.Count, entry.Offset);
}

journal.Dispose();
Console.WriteLine("Journal Disposed");