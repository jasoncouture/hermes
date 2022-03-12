using System.Collections.Concurrent;

namespace Journal.Background;

public class PeriodTaskManager : IPeriodicTaskRunner, IPeriodicTaskTracker
{
    private ConcurrentQueue<WeakReference> _tasks = new ConcurrentQueue<WeakReference>();


    public void Register(IPeriodicTask periodicTask)
    {
        _tasks.Enqueue(new WeakReference(periodicTask));
    }

    public IPeriodicTask? GetNextTask(CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_tasks.IsEmpty)
            {
                return null;
            }

            if (!_tasks.TryDequeue(out var next)) continue;
            if (next.Target is IPeriodicTask typedReference) return typedReference;
        }
    }
}