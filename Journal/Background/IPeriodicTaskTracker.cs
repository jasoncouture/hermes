namespace Journal.Background;

public interface IPeriodicTaskTracker
{
    IPeriodicTask? GetNextTask(CancellationToken cancellationToken);
}