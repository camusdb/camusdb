/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

/// <summary>
/// Request to create a user sequence.
///
/// <para>Every parameter is already resolved to a number here: the parser turns the option words
/// into these fields, so the validator and the executor never re-read the SQL text and the two
/// cannot disagree about what the statement said.</para>
/// </summary>
public readonly struct CreateSequenceTicket
{
    public string DatabaseName { get; }

    public string SequenceName { get; }

    /// <summary>The first value the sequence issues.</summary>
    public long StartValue { get; }

    /// <summary>The step between values. Always positive.</summary>
    public long Increment { get; }

    /// <summary>The lowest value the sequence may hold. Enforced by CamusDB, not by the counter.</summary>
    public long MinValue { get; }

    /// <summary>The highest value the sequence may issue, or null for no ceiling.</summary>
    public long? MaxValue { get; }

    /// <summary>
    /// Values reserved per durable commit, or null to follow the node-wide setting. <c>1</c> means
    /// gap-free, at the cost of one commit with its fsync per value.
    /// </summary>
    public int? CacheSize { get; }

    /// <summary>True when <c>IF NOT EXISTS</c> was written: an existing name is success, not an error.</summary>
    public bool IfNotExists { get; }

    public CreateSequenceTicket(
        string databaseName,
        string sequenceName,
        long startValue,
        long increment,
        long minValue,
        long? maxValue,
        int? cacheSize,
        bool ifNotExists)
    {
        DatabaseName = databaseName;
        SequenceName = sequenceName;
        StartValue = startValue;
        Increment = increment;
        MinValue = minValue;
        MaxValue = maxValue;
        CacheSize = cacheSize;
        IfNotExists = ifNotExists;
    }
}

/// <summary>Request to remove a user sequence.</summary>
public readonly struct DropSequenceTicket
{
    public string DatabaseName { get; }

    public string SequenceName { get; }

    /// <summary>True when <c>IF EXISTS</c> was written: a missing sequence is success, not an error.</summary>
    public bool IfExists { get; }

    public DropSequenceTicket(string databaseName, string sequenceName, bool ifExists)
    {
        DatabaseName = databaseName;
        SequenceName = sequenceName;
        IfExists = ifExists;
    }
}

/// <summary>Request to rename a user sequence. Metadata-only: the counter does not move.</summary>
public readonly struct RenameSequenceTicket
{
    public string DatabaseName { get; }

    public string SequenceName { get; }

    public string NewName { get; }

    public RenameSequenceTicket(string databaseName, string sequenceName, string newName)
    {
        DatabaseName = databaseName;
        SequenceName = sequenceName;
        NewName = newName;
    }
}

/// <summary>
/// Request to change a live sequence. Carries only the parameters the statement named; a null
/// field means "leave this as the record has it".
///
/// <para>The removal flags exist because two of the fields are nullable at rest. A null
/// <see cref="MaxValue"/> alone cannot say whether the statement wrote <c>NO MAXVALUE</c> or did
/// not mention the maximum at all.</para>
/// </summary>
public readonly struct AlterSequenceTicket
{
    public string DatabaseName { get; }

    public string SequenceName { get; }

    /// <summary>The recorded start value, moved by <c>START WITH</c>. Does not move the counter.</summary>
    public long? StartValue { get; }

    public long? Increment { get; }

    public long? MinValue { get; }

    public bool RemoveMinValue { get; }

    public long? MaxValue { get; }

    public bool RemoveMaxValue { get; }

    public int? CacheSize { get; }

    public bool RemoveCacheSize { get; }

    /// <summary>
    /// True when <c>RESTART</c> was written. The counter is moved so the next value issued is
    /// <see cref="RestartWith"/>, or the sequence's recorded start value when that is null.
    /// </summary>
    public bool Restart { get; }

    /// <summary>The value the next <c>nextval</c> should return, or null for the recorded start value.</summary>
    public long? RestartWith { get; }

    public AlterSequenceTicket(
        string databaseName,
        string sequenceName,
        long? startValue,
        long? increment,
        long? minValue,
        bool removeMinValue,
        long? maxValue,
        bool removeMaxValue,
        int? cacheSize,
        bool removeCacheSize,
        bool restart,
        long? restartWith)
    {
        DatabaseName = databaseName;
        SequenceName = sequenceName;
        StartValue = startValue;
        Increment = increment;
        MinValue = minValue;
        RemoveMinValue = removeMinValue;
        MaxValue = maxValue;
        RemoveMaxValue = removeMaxValue;
        CacheSize = cacheSize;
        RemoveCacheSize = removeCacheSize;
        Restart = restart;
        RestartWith = restartWith;
    }

    /// <summary>True when the statement named nothing the engine could act on.</summary>
    public bool IsEmpty =>
        StartValue is null && Increment is null && MinValue is null && !RemoveMinValue
        && MaxValue is null && !RemoveMaxValue && CacheSize is null && !RemoveCacheSize && !Restart;
}
