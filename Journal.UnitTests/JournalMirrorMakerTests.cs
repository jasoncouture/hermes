using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Journal.Background;
using Journal.Encoding;
using Journal.Journal.Remote;
using Xunit;
using Xunit.Abstractions;

namespace Journal.UnitTests;

public class JournalMirrorMakerTests
{
    private readonly ITestOutputHelper _testOutputHelper;

    public JournalMirrorMakerTests(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    [Fact]
    public async Task DataPublishedToTheSourceIsMirroredToPeer()
    {
        var journalDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        var sourceDirectory = Path.Combine(journalDirectory, "source");
        var mirrorDirectory = Path.Combine(journalDirectory, "mirror");
        if (Directory.Exists(journalDirectory))
            Directory.Delete(journalDirectory, true);
        Directory.CreateDirectory(sourceDirectory);
        Directory.CreateDirectory(mirrorDirectory);
        
        CancellationTokenSource tokenSource = new CancellationTokenSource();
        var cancellationToken = tokenSource.Token;
        
        var taskManager = new PeriodTaskManager();
        
        async Task TaskProcessor()
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await RunPendingPeriodicTasks(taskManager).ConfigureAwait(false);
                await Task.Delay(50, cancellationToken).ConfigureAwait(false);
            }
        }

        HashSet<long> offsetsWrittenToSource = new();
        var taskManagerTask = TaskProcessor();
        var sourceJournal = new Journal.LocalJournal(sourceDirectory, taskManager);
        var mirrorJournal = new Journal.LocalJournal(mirrorDirectory, taskManager);
        var mirrorMaker = new MirrorMaker(sourceJournal, mirrorJournal);
        var mirrorMakerTask = mirrorMaker.RunAsync(cancellationToken);
        long lastOffset = 0;
        for (var x = 0; x < 100; x++)
        {
            lastOffset = await sourceJournal.WriteEntryAsync(x.ToBytes(), cancellationToken);
            offsetsWrittenToSource.Add(lastOffset);
        }

        await sourceJournal.WaitForCommit(CancellationToken.None).ConfigureAwait(false);
        
        await foreach (var entry in mirrorJournal.ScanJournalAsync(-1, lastOffset, cancellationToken))
        {
            _testOutputHelper.WriteLine("Read entry at offset {0}", entry.Offset);
            Assert.Contains(entry.Offset, offsetsWrittenToSource);
            Assert.True(offsetsWrittenToSource.Remove(entry.Offset));
        }
        
        Assert.Empty(offsetsWrittenToSource);
        
        tokenSource.Cancel();
        try
        {
            await Task.WhenAll(taskManagerTask, mirrorMakerTask).ConfigureAwait(false);
        }
        catch
        {
            // ignored. Exiting is what we wanted.
        }
        
        mirrorJournal.Dispose();
        sourceJournal.Dispose();
        Directory.Delete(journalDirectory, true);
    }

    private async Task RunPendingPeriodicTasks(PeriodTaskManager taskManager)
    {
        for (var x = 0; x < 3; x++)
        {
            var next = taskManager.GetNextTask(CancellationToken.None);
            if (next == null) continue;
            next = await next.RunAsync(CancellationToken.None);
            if (next == null) continue;
            taskManager.Register(next);
        }
    }
}