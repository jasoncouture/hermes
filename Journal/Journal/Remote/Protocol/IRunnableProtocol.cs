namespace Journal.Journal.Remote.Protocol;

public interface IRunnableProtocol : IProtocol
{
    Task RunAsync(CancellationToken cancellationToken);
}