namespace Hermes.Journal;

public record JournalEntry(long SequenceNumber, int Partition, byte[]? Key, byte[]? Data,
    IEnumerable<JournalEntryHeader> JournalEntryHeaders);