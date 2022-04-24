namespace Hermes.Journal;

public record JournalEntryHeader(string Key, IEnumerable<string> Values);