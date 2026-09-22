/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsValidator.Validators;

/// <summary>
/// Rejects a sequence statement that cannot hold, before any schema mutation runs.
///
/// <para>Every check here is about the definition itself rather than about the database's state —
/// whether a name is taken, or whether a column depends on the sequence, is decided under the
/// schema lock at execution time, because a lock-free answer to either is a check-then-act that
/// two concurrent statements both pass.</para>
/// </summary>
internal sealed class SequenceValidator : ValidatorBase
{
    public SequenceValidator(CamusDBOptions options) : base(options) { }

    public void Validate(CreateSequenceTicket ticket)
    {
        RequireDatabase(ticket.DatabaseName);
        ValidateIdentifier(ticket.SequenceName, "Sequence name");
        ValidateNotReservedRelationName(ticket.SequenceName, "sequence");

        RequirePositiveIncrement(ticket.Increment);
        RequireCacheSize(ticket.CacheSize);
        RequireBoundsHold(ticket.MinValue, ticket.MaxValue, ticket.StartValue, ticket.Increment);
    }

    public void Validate(DropSequenceTicket ticket)
    {
        RequireDatabase(ticket.DatabaseName);
        ValidateIdentifier(ticket.SequenceName, "Sequence name");
    }

    public void Validate(RenameSequenceTicket ticket)
    {
        RequireDatabase(ticket.DatabaseName);
        ValidateIdentifier(ticket.SequenceName, "Sequence name");
        ValidateIdentifier(ticket.NewName, "Sequence name");
        ValidateNotReservedRelationName(ticket.NewName, "sequence");
    }

    /// <summary>
    /// Checks an <c>ALTER SEQUENCE</c> in isolation. Bounds that mix a new value with a recorded
    /// one — a <c>MAXVALUE</c> lowered below a minimum the statement did not mention — cannot be
    /// checked here, because the recorded values are not in the ticket. The executor re-checks the
    /// folded definition under the schema lock for that reason.
    /// </summary>
    public void Validate(AlterSequenceTicket ticket)
    {
        RequireDatabase(ticket.DatabaseName);
        ValidateIdentifier(ticket.SequenceName, "Sequence name");

        if (ticket.IsEmpty)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidSequenceDefinition,
                $"ALTER SEQUENCE '{ticket.SequenceName}' named nothing to change");

        if (ticket.Increment is { } increment)
            RequirePositiveIncrement(increment);

        RequireCacheSize(ticket.CacheSize);

        if (ticket.MinValue is { } minValue && ticket.MaxValue is { } maxValue && maxValue < minValue)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidSequenceDefinition,
                $"MAXVALUE ({maxValue}) is below MINVALUE ({minValue})");
    }

    private static void RequireDatabase(string databaseName)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Database name is required");
    }

    /// <summary>
    /// A sequence must climb. A zero step never advances, and a negative one would make a
    /// descending sequence — which the counter behind this cannot produce, so accepting the
    /// spelling would promise behavior the engine has no way to deliver.
    /// </summary>
    private static void RequirePositiveIncrement(long increment)
    {
        if (increment > 0)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidSequenceDefinition,
            increment == 0
                ? "INCREMENT must not be 0: a sequence that never advances cannot issue a second value"
                : $"INCREMENT must be positive, got {increment}. Descending sequences are not supported.");
    }

    private static void RequireCacheSize(int? cacheSize)
    {
        if (cacheSize is { } cache && cache < 1)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidSequenceDefinition,
                $"CACHE must be at least 1, got {cache}");
    }

    /// <summary>
    /// The definition check, shared by <c>CREATE</c> and by the folded result of an <c>ALTER</c>.
    /// </summary>
    /// <remarks>
    /// An <c>ALTER</c> has to be checked against the definition it <em>produces</em>, not against
    /// the fields it names: lowering a maximum below a minimum the statement never mentioned, or
    /// raising a minimum above a start value it never mentioned, are exactly the contradictions a
    /// ticket-only check cannot see.
    /// </remarks>
    internal static void RequireDefinitionHolds(long minValue, long? maxValue, long startValue, long increment)
    {
        RequirePositiveIncrement(increment);
        RequireBoundsHold(minValue, maxValue, startValue, increment);
    }

    /// <summary>
    /// Checks that the three bounds describe a sequence that can issue at least one value, and
    /// that the counter can be seeded for it.
    /// </summary>
    /// <remarks>
    /// The underflow check is not hypothetical bookkeeping. The counter is created holding
    /// <c>start - increment</c> so that the first value it issues is the start value itself, and a
    /// start near <see cref="long.MinValue"/> would make that subtraction wrap to a huge positive
    /// number — a sequence that silently begins near the top of the range instead of at the bottom.
    /// </remarks>
    private static void RequireBoundsHold(long minValue, long? maxValue, long startValue, long increment)
    {
        if (maxValue is { } max && max < minValue)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidSequenceDefinition,
                $"MAXVALUE ({max}) is below MINVALUE ({minValue})");

        if (startValue < minValue)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidSequenceDefinition,
                $"START ({startValue}) is below MINVALUE ({minValue})");

        if (maxValue is { } ceiling && startValue > ceiling)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidSequenceDefinition,
                $"START ({startValue}) is above MAXVALUE ({ceiling})");

        if (startValue - increment > startValue)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidSequenceDefinition,
                $"START ({startValue}) is too close to the smallest 64-bit integer to seed the counter; " +
                $"it must be at least {long.MinValue} + INCREMENT ({increment})");
    }
}
