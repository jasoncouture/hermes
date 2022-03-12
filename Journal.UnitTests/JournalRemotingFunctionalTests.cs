using System;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Journal.Background;
using Journal.Encoding;
using Journal.Journal;
using Journal.Journal.Remote;
using Journal.Journal.Remote.Protocol;
using Xunit;

namespace Journal.UnitTests;

public class JournalRemotingFunctionalTests
{
    [Fact]
    public async Task CanPublishAndScanViaRemote()
    {
        var journalDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        CancellationTokenSource tokenSource = new CancellationTokenSource();
        var cancellationToken = tokenSource.Token;
        if (Directory.Exists(journalDirectory))
            Directory.Delete(journalDirectory, true);
        Directory.CreateDirectory(journalDirectory);
        var taskManager = new PeriodTaskManager();
        var journal = new Journal.LocalJournal(journalDirectory, taskManager);
        var pipe = new Pipe(); // Pipe it to itself :D
        var protocol = new PipelineProtocol(pipe.Reader, pipe.Writer);
        var pipeRunTask = protocol.RunAsync(cancellationToken);
        var remoteJournal = new RemoteJournalClient(protocol);
        var server = new RemoteJournalServer(journal, protocol);
        var serverTask = server.RunAsync(cancellationToken);
        var tasks = Enumerable.Range(0, 3)
            .Select(i => remoteJournal.WriteEntryAsync(i.ToBytes(), cancellationToken).AsTask()).ToArray();
        await Task.WhenAll(tasks);
        var logOffset =
            await remoteJournal.WriteEntryAsync(new ArraySegment<byte>(new byte[1]), cancellationToken);
        var commitTask = remoteJournal.WaitForCommit(cancellationToken);
        var periodicTask = taskManager.GetNextTask(cancellationToken);
        for (var x = 0; x < 10; x++)
        {
            var nextTask = await periodicTask.RunAsync(cancellationToken);
            Assert.NotNull(nextTask);
            taskManager.Register(nextTask);
            periodicTask = nextTask;
        }

        await commitTask;
        JournalData? lastEntry = null;
        await foreach (var entry in remoteJournal.ScanJournalAsync(0, logOffset, cancellationToken))
        {
            lastEntry = entry;
        }

        tokenSource.Cancel();
        try
        {
            await serverTask;
        }
        catch (OperationCanceledException)
        {
        }

        try
        {
            await pipeRunTask;
        }
        catch (OperationCanceledException)
        {
        }

        Assert.NotNull(lastEntry);
        Assert.Equal(lastEntry.Offset, logOffset);
        
        journal.Dispose();
        Directory.Delete(journalDirectory, true);
    }
}