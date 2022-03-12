using System.Collections.Concurrent;
using System.Threading.Channels;
using Journal.Encoding;
using Journal.Journal.Remote.Protocol;

namespace Journal.Journal.Remote;

public class RemoteJournalServer
{
    private readonly IJournal _journal;
    private readonly IProtocol _protocol;
    private long _streamId = 0;
    private ConcurrentDictionary<long, Channel<JournalData>> _activeStreams = new();
    private List<Task> _tasks = new();

    public RemoteJournalServer(IJournal journal, IProtocol protocol)
    {
        _journal = journal;
        _protocol = protocol;
    }

    private long CreateStream()
    {
        var id = GetNextStreamId();
        _activeStreams.TryAdd(id,
            Channel.CreateBounded<JournalData>(new BoundedChannelOptions(32)
                {FullMode = BoundedChannelFullMode.Wait, SingleWriter = true, SingleReader = true}));
        return id;
    }

    private void EndStream(long id)
    {
        if(!_activeStreams.TryRemove(id, out var channel)) return;
        channel.Writer.TryComplete();
        // Drain the channel so the GC can collect this stuff.
        while (channel.Reader.TryRead(out _))
        {
            
        }
    }

    private Channel<JournalData>? GetStream(long id)
    {
        _activeStreams.TryGetValue(id, out var channel);
        return channel;
    }

    private long GetNextStreamId() => Interlocked.Increment(ref _streamId);

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var completedTasks = _tasks.Where(i => i.IsCompleted);
            foreach (var task in completedTasks.ToArray())
            {
                await CompleteTaskWithoutException(task);
                _tasks.Remove(task);
            }

            var nextCommand = await _protocol.ReceiveCommandAsync(cancellationToken);
            if (await ProcessCommand(nextCommand, cancellationToken)) continue;
            break;
        }
    }

    private async Task CompleteTaskWithoutException(Task task)
    {
        try
        {
            await task;
        }
        catch
        {
            // Do nothing.
        }
    }

    private async Task<bool> ProcessCommand(RemoteJournalCommand nextCommand, CancellationToken cancellationToken)
    {
        switch (nextCommand.Type)
        {
            case RemoteJournalCommandType.WriteEntry:
                await HandleWriteEntry(nextCommand.StreamId, nextCommand.Segments.ToArray(), cancellationToken);
                break;
            case RemoteJournalCommandType.WaitForCommit:
                await WaitForCommit(nextCommand.StreamId, cancellationToken);
                break;
            case RemoteJournalCommandType.ScanStart:
                await HandleScanStart(nextCommand.StreamId, nextCommand.Segments.ToArray(), cancellationToken);
                break;
            case RemoteJournalCommandType.ScanNext:
                await HandleScanNext(nextCommand.StreamId, nextCommand.Segments.ToArray(), cancellationToken);
                break;
            case RemoteJournalCommandType.ScanStop:
                await HandleScanStop(nextCommand.StreamId, nextCommand.Segments.ToArray(), cancellationToken);
                break;
            default:
                await _protocol.SendResponseAsync(new RemoteJournalResponse(~nextCommand.StreamId,
                    System.Text.Encoding.UTF8.GetBytes("NOT IMPL")), cancellationToken);
                break;
        }
        return true;
    }

    private async Task Scan(long streamId, long startOffset, long endOffset, CancellationToken cancellationToken)
    {
        var channel = GetStream(streamId);
        if (channel == null) return;
        try
        {
            await foreach (var item in _journal.ScanJournalAsync(startOffset, endOffset, cancellationToken))
            {
                await channel.Writer.WriteAsync(item, cancellationToken);
            }
        }
        catch(Exception ex)
        {
            channel.Writer.TryComplete(ex);
        }
        finally
        {
            channel.Writer.TryComplete();
        }
    }

    private async Task HandleScanStop(long nextCommandStreamId, ArraySegment<byte>[] arguments,
        CancellationToken cancellationToken)
    {
        var streamId = arguments[0].ToInt64();
        EndStream(streamId);
        await _protocol.SendResponseAsync(new RemoteJournalResponse(nextCommandStreamId), cancellationToken);
    }

    private async Task HandleScanNext(long nextCommandStreamId, ArraySegment<byte>[] arguments,
        CancellationToken cancellationToken)
    {
        var streamId = arguments[0].ToInt64();
        var channel = GetStream(streamId);
        if (channel == null)
        {
            await _protocol.SendResponseAsync(new RemoteJournalResponse(nextCommandStreamId), cancellationToken);
            return;
        }

        var items = new List<ReadOnlyMemory<byte>>();
        while (items.Count < 32 && channel.Reader.TryRead(out var next))
        {
            items.Add(MakeBlob(next));
        }
        // We add an empty byte[] to indicate that we do not have any data, but the stream is still open.
        // This indicates the client should wait a little bit before polling again.
        await _protocol.SendResponseAsync(new RemoteJournalResponse(nextCommandStreamId, items.DefaultIfEmpty(Array.Empty<byte>())),
            cancellationToken);
    }

    private byte[] MakeBlob(JournalData data)
    {
        var buffer = new byte[data.Size];
        data.WriteTo(buffer);
        return buffer;
    }

    private async Task HandleScanStart(long nextCommandStreamId, ArraySegment<byte>[] arguments,
        CancellationToken cancellationToken)
    {
        var startOffset = arguments[0].ToInt64();
        var endOffset = arguments[1].ToInt64();
        var streamId = CreateStream();
        _tasks.Add(Scan(streamId, startOffset, endOffset, cancellationToken));
        await _protocol.SendResponseAsync(new RemoteJournalResponse(nextCommandStreamId, streamId.ToBytes()),
            cancellationToken);
    }

    private async Task WaitForCommit(long streamId, CancellationToken cancellationToken)
    {
        await _journal.WaitForCommit(cancellationToken);
        await _protocol.SendResponseAsync(new RemoteJournalResponse(streamId), cancellationToken);
    }

    private async Task HandleWriteEntry(long streamId, ArraySegment<byte>[] args, CancellationToken cancellationToken)
    {
        var data = args[0];
        var virtualAddress = args[1].ToInt64();
        var address = await _journal.WriteEntryAsync(data, virtualAddress, cancellationToken);
        await _protocol.SendResponseAsync(new RemoteJournalResponse(streamId, address.ToBytes()), cancellationToken);
    }
}