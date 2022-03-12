using Microsoft.Extensions.Hosting;

namespace Journal.Background;

public class PeriodicTaskRunner : BackgroundService
{
    private readonly IPeriodicTaskTracker _tracker;
    private readonly IPeriodicTaskRunner _runner;

    public PeriodicTaskRunner(IPeriodicTaskTracker tracker, IPeriodicTaskRunner runner)
    {
        _tracker = tracker;
        _runner = runner;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(500));
        List<Task<IPeriodicTask?>> runningJobs = new();
        while (!stoppingToken.IsCancellationRequested)
        {
            await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false);
            while (true)
            {
                var next = _tracker.GetNextTask(stoppingToken);
                if (next == null) break;
                runningJobs.Add(RunAsync(next, stoppingToken));
            }

            if (runningJobs.Count == 0) continue;
            var results = await Task.WhenAll(runningJobs);
            foreach (var result in results)
            {
                if (result is null) continue;
                _runner.Register(result);
            }

            runningJobs.Clear();
        }
    }

    private async Task<IPeriodicTask?> RunAsync(IPeriodicTask task, CancellationToken cancellationToken)
    {
        try
        {
            return await task.RunAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            return null;
        }
        catch
        {
            return task;
        }
    }
}