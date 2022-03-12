using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Threading.Channels;
using Journal.Encoding;

namespace Journal.Journal.Remote.Protocol;

public class PipelineProtocol : IRunnableProtocol
{
    private readonly PipeReader _reader;
    private readonly PipeWriter _writer;

    private Channel<ArraySegment<byte>> _outgoingPacketChannel = Channel.CreateBounded<ArraySegment<byte>>(
        new BoundedChannelOptions(16)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = false,
            SingleReader = true
        });

    private Channel<RemoteJournalCommand> _incomingCommandChannel =
        Channel.CreateBounded<RemoteJournalCommand>(new BoundedChannelOptions(16)
        {
            SingleWriter = true, 
            SingleReader = false
        });

    private readonly ConcurrentDictionary<long, RemoteJournalResponse> _receivedResponses = new();


    public PipelineProtocol(PipeReader reader, PipeWriter writer)
    {
        _reader = reader;
        _writer = writer;
    }

    public async Task<RemoteJournalCommand> ReceiveCommandAsync(CancellationToken cancellationToken)
    {
        return await _incomingCommandChannel.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task SendResponseAsync(RemoteJournalResponse response, CancellationToken cancellationToken)
    {
        // Wait before we allocate the memory, just in case we're blocked.
        var buffer = new ArraySegment<byte>(ArrayPool<byte>.Shared.Rent(response.Size), 0, response.Size);
        response.WriteTo(buffer);
        await _outgoingPacketChannel.Writer.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
    }

    public async Task SendCommandAsync(RemoteJournalCommand command, CancellationToken cancellationToken)
    {
        // Wait before we allocate the memory, just in case we're blocked.
        var buffer = new ArraySegment<byte>(ArrayPool<byte>.Shared.Rent(command.Size), 0, command.Size);
        command.WriteTo(buffer);
        await _outgoingPacketChannel.Writer.WriteAsync(buffer, cancellationToken);
    }

    public async Task<RemoteJournalResponse> ReceiveResponseAsync(RemoteJournalCommand command,
        CancellationToken cancellationToken)
    {
        RemoteJournalResponse? next;
        while (!_receivedResponses.TryRemove(command.StreamId, out next))
        {
            await Task.Delay(5, cancellationToken).ConfigureAwait(false);
        }

        return next;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var readerTask = RunReaderAsync(cancellationToken);
        var writerTask = RunWriterAsync(cancellationToken);
        await Task.WhenAll(readerTask, writerTask);
    }

    private async Task RunWriterAsync(CancellationToken cancellationToken)
    {
        await foreach (var packet in _outgoingPacketChannel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            var flushResult = await _writer.WriteAsync(packet, cancellationToken).ConfigureAwait(false);
            if (packet.Array != null)
                ArrayPool<byte>.Shared.Return(packet.Array);
            if (flushResult.IsCompleted)
            {
                await _writer.CompleteAsync();
                _outgoingPacketChannel.Writer.TryComplete();
                break;
            }
        }
    }

    private async Task RunReaderAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            var readResult = await _reader.ReadAsync(cancellationToken);

            var buffer = readResult.Buffer;

            while (buffer.Length > 4)
            {
                bool isCommand = false;
                var length = buffer.Slice(0, 4).ToArray().ToInt32();
                if (length > 0) isCommand = true;
                if (!isCommand) length = ~length;
                if (length <= buffer.Length)
                {
                    var packet = buffer.Slice(0, length).ToArray();
                    if (isCommand)
                    {
                        var command = RemoteJournalCommand.ReadFrom(packet);
                        await _incomingCommandChannel.Writer.WriteAsync(command, cancellationToken);
                    }
                    else
                    {
                        var response = RemoteJournalResponse.ReadFrom(packet);
                        _receivedResponses.TryAdd(response.StreamId, response);
                    }

                    buffer = buffer.Slice(length);
                }
                else
                {
                    break;
                }
            }

            _reader.AdvanceTo(buffer.Start, buffer.End);
            if (readResult.IsCompleted) break;
        }

        await _reader.CompleteAsync();
    }
}