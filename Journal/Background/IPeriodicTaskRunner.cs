namespace Journal.Background;

public interface IPeriodicTaskRunner
{
    void Register(IPeriodicTask periodicTask);
}