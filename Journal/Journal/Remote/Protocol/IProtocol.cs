namespace Journal.Journal.Remote.Protocol;

public interface IProtocol
{
    Task<RemoteJournalCommand> ReceiveCommandAsync(CancellationToken cancellationToken);

    Task SendResponseAsync(RemoteJournalResponse response, CancellationToken cancellationToken);
    Task SendCommandAsync(RemoteJournalCommand command, CancellationToken cancellationToken);
    Task<RemoteJournalResponse> ReceiveResponseAsync(RemoteJournalCommand command, CancellationToken cancellationToken);
}