using System.Buffers;
using System.Runtime.CompilerServices;
using FASTER.core;
using Journal.Background;

namespace Journal.Journal;

public class LocalJournal : IJournal, IPeriodicTask, IDisposable
{
    private readonly OffsetMap _offsetMap;
    private readonly FasterLog _log;
    private readonly string _offsetMapSnapshotFile;
    private readonly CancellationTokenSource _disposedToken = new();
    private int _lastMapSize;

    public LocalJournal(string path, IPeriodicTaskRunner journalPeriodicManager)
    {
        var directory = Directory.CreateDirectory(path);
        var filename = Path.Combine(directory.FullName, "partition.log");
        var snapshotFile = Path.Combine(directory.FullName, "offset.map");
        _offsetMapSnapshotFile = snapshotFile;
        if (File.Exists(snapshotFile))
        {
            var bytes = File.ReadAllBytes(snapshotFile);
            _offsetMap = OffsetMap.FromSnapshot(bytes);
        }
        else
        {
            _offsetMap = new OffsetMap();
        }

        var device = Devices.CreateLogDevice(filename);
        _log = new FasterLog(new FasterLogSettings() {LogDevice = device, LogChecksum = LogChecksumType.PerEntry});
        journalPeriodicManager.Register(this);
    }

    private async Task SnapshotOffsetMap(CancellationToken cancellationToken)
    {
        var tempFile = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        await File.WriteAllBytesAsync(tempFile, _offsetMap.GetSnapshot(), cancellationToken);
        if (cancellationToken.IsCancellationRequested) throw new OperationCanceledException(cancellationToken);
        File.Move(tempFile, _offsetMapSnapshotFile, true);
    }

    public async ValueTask<long> WriteEntryAsync(ArraySegment<byte> data, CancellationToken cancellationToken)
    {
        return await WriteEntryAsync(data, 0, cancellationToken);
    }

    public async ValueTask<long> WriteEntryAsync(ArraySegment<byte> data, long virtualAddress,
        CancellationToken cancellationToken)
    {
        var logEntry = new JournalData(virtualAddress, data);
        var buffer = ArrayPool<byte>.Shared.Rent(logEntry.Size);
        try
        {
            var segment = new ArraySegment<byte>(buffer, 0, logEntry.Size);
            logEntry.WriteTo(segment);
            var fileOffset = await _log.EnqueueAsync(segment, cancellationToken);
            await _log.RefreshUncommittedAsync(cancellationToken);
            if (virtualAddress == 0) virtualAddress = fileOffset;
            _offsetMap.Map(fileOffset, virtualAddress);
            return virtualAddress;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private volatile bool dirty = false;

    public async ValueTask WaitForCommit(CancellationToken cancellationToken)
    {
        if (dirty == false) return;
        dirty = false;
        await _log.WaitForCommitAsync(0, cancellationToken);
        await _log.RefreshUncommittedAsync(cancellationToken);
    }

    public async IAsyncEnumerable<JournalData> ScanJournalAsync(long startOffset = -1, long endOffset = -1,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (startOffset < 0) startOffset = 0;
        startOffset = _offsetMap.GetFileOffset(startOffset);
        if (endOffset < 0)
        {
            endOffset = long.MaxValue;
        }
        else if (endOffset == 0)
        {
            endOffset = _log.TailAddress;
        }
        else
        {
            endOffset = _offsetMap.GetFileOffset(endOffset);
        }

        if (_log.BeginAddress == _log.TailAddress && endOffset == 0 || endOffset == _log.TailAddress) yield break;

        if (endOffset < startOffset) yield break;
        if (endOffset == startOffset && _log.BeginAddress == startOffset) yield break;

        using var iter = _log.Scan(startOffset, long.MaxValue, recover: false);
        await foreach (var (entry, _, currentAddress, nextAddress) in iter.GetAsyncEnumerable(cancellationToken))
        {
            if (currentAddress > endOffset) yield break;
            if (entry != null)
                yield return JournalData.ReadFrom(currentAddress, entry);
            iter.CompleteUntil(nextAddress);
            if (currentAddress == endOffset) yield break;
        }
    }

    public void Dispose()
    {
        if (_disposedToken.IsCancellationRequested) return;
        _disposedToken.Cancel();
        _log.Dispose();
    }

    public async Task<IPeriodicTask?> RunAsync(CancellationToken cancellationToken)
    {
        if (_disposedToken.IsCancellationRequested) return null;
        var linkedToken = CancellationTokenSource.CreateLinkedTokenSource(_disposedToken.Token, cancellationToken);
        if (linkedToken.IsCancellationRequested) return null;
        if (_lastMapSize != _offsetMap.Count)
        {
            _lastMapSize = _offsetMap.Count;
            await SnapshotOffsetMap(linkedToken.Token);
        }

        await _log.CommitAsync(linkedToken.Token);


        return this;
    }
}