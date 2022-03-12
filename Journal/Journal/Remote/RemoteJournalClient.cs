using Journal.Encoding;
using Journal.Journal.Remote.Protocol;

namespace Journal.Journal.Remote;

public class RemoteJournalClient : IJournal
{
    private long _commandStreamId = 0;
    private IProtocol _protocol;

    public RemoteJournalClient(IProtocol protocol)
    {
        _protocol = protocol;
    }

    private long GetNextCommandStreamId() => Interlocked.Increment(ref _commandStreamId);

    public async ValueTask<long> WriteEntryAsync(ArraySegment<byte> data, CancellationToken cancellationToken)
    {
        return await WriteEntryAsync(data, 0, cancellationToken);
    }

    public async ValueTask<long> WriteEntryAsync(ArraySegment<byte> data, long virtualAddress,
        CancellationToken cancellationToken)
    {
        var command = new RemoteJournalCommand(GetNextCommandStreamId(), RemoteJournalCommandType.WriteEntry,
            new[] {data, new ArraySegment<byte>(virtualAddress.ToBytes())});
        await _protocol.SendCommandAsync(command, cancellationToken);
        RemoteJournalResponse response = await _protocol.ReceiveResponseAsync(command, cancellationToken);
        return response.Segments.Single().ToInt64();
    }

    public async ValueTask WaitForCommit(CancellationToken cancellationToken)
    {
        var command = new RemoteJournalCommand(GetNextCommandStreamId(), RemoteJournalCommandType.WaitForCommit);
        await _protocol.SendCommandAsync(command, cancellationToken);
        await _protocol.ReceiveResponseAsync(command, cancellationToken);
    }

    public async IAsyncEnumerable<JournalData> ScanJournalAsync(long startOffset = -1, long endOffset = -1,
        CancellationToken cancellationToken = default)
    {
        var command = new RemoteJournalCommand(GetNextCommandStreamId(), RemoteJournalCommandType.ScanStart, new(startOffset.ToBytes()), new(endOffset.ToBytes()));
        await _protocol.SendCommandAsync(command, cancellationToken);
        var response = await _protocol.ReceiveResponseAsync(command, cancellationToken);
        var serverStreamId = response.Segments.Single().ToArray();
        try
        {
            var getNextCommand = new RemoteJournalCommand(command.StreamId, RemoteJournalCommandType.ScanNext,
                serverStreamId);
            while (true)
            {
                await _protocol.SendCommandAsync(getNextCommand, cancellationToken);
                response = await _protocol.ReceiveResponseAsync(getNextCommand, cancellationToken);
                if (response.Count == 0) break;
                bool complete = false;
                foreach (var dataPacket in response.Segments)
                {
                    // This indicates the stream is open, but has no data. Wait 100ms before trying again.
                    if (dataPacket.Length == 0)
                    {
                        await Task.Delay(100, cancellationToken);
                        continue;
                    }
                    var data = JournalData.ReadFrom(0, dataPacket);
                    
                    yield return data;
                    if (data.Offset >= endOffset)
                    {
                        complete = true;
                        break;
                    }
                }

                if (complete) break;
            }
        }
        finally
        {
            var stopCommand =
                new RemoteJournalCommand(command.StreamId, RemoteJournalCommandType.ScanStop, serverStreamId);
            await _protocol.SendCommandAsync(stopCommand, cancellationToken);
        }
    }
}