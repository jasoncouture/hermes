namespace Journal.Background;

public interface IPeriodicTask
{
    Task<IPeriodicTask?> RunAsync(CancellationToken cancellationToken);
}