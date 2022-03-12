namespace Journal.Journal.Remote.Protocol;

public enum RemoteJournalCommandType : int
{
    WriteEntry,
    WaitForCommit,
    ScanStart,
    ScanNext,
    ScanStop
}