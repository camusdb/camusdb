/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Executes <c>CREATE SEQUENCE</c>, <c>DROP SEQUENCE</c>, <c>ALTER SEQUENCE</c> and
/// <c>ALTER SEQUENCE … RENAME TO</c>.
///
/// <para><b>A sequence is two objects, and the order they are touched in is the whole design.</b>
/// The catalog record is a replicated schema delta; the counter is a Kahuna sequencer record that
/// cannot join a transaction and cannot be written from the apply callback. So the two cannot be
/// made atomic, and this class chooses which side goes first in each direction:</para>
///
/// <list type="bullet">
/// <item><description><b>Create: record first, counter second.</b> A crash in between leaves a
/// record naming a counter that does not exist, and the first <c>nextval</c> creates it from the
/// record's start value (<see cref="SequenceAllocator.ReserveAsync"/>). The other order would
/// leave a counter nothing names — unreachable and never reclaimed.</description></item>
/// <item><description><b>Drop: record first, counter second.</b> A crash in between leaves a
/// counter whose id is gone from the catalog, so nothing can ever name it again. That is a leak
/// rather than a hazard, and the whole-database purge sweeps it when the database goes.
/// Deleting the counter first would let a live record hand out values from a counter that was
/// about to be recreated at zero.</description></item>
/// <item><description><b>Restart: counter first, record second.</b> The counter move is the part
/// that can fail slowly or be refused; doing it first means a failure leaves the recorded
/// parameters describing what the counter actually is.</description></item>
/// </list>
///
/// <para><b>A restart waits about five seconds.</b> Kahuna withholds a sequence update until no
/// stale block from the replaced incarnation can still be served. That is what makes moving a live
/// counter safe instead of a hole in the uniqueness guarantee, and it is why restarting is a
/// DDL-shaped operation rather than something to call per statement.</para>
/// </summary>
internal sealed class SequenceDdlService
{
    private readonly ExecutorContext context;

    private readonly CatalogsManager catalogs;

    private readonly SequenceAllocator allocator;

    internal SequenceDdlService(ExecutorContext context, CatalogsManager catalogs, SequenceAllocator allocator)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(catalogs);
        ArgumentNullException.ThrowIfNull(allocator);

        this.context = context;
        this.catalogs = catalogs;
        this.allocator = allocator;
    }

    /// <summary>
    /// Creates a sequence. Returns false only when <c>IF NOT EXISTS</c> was written and the name is
    /// already taken by a sequence; every other failure raises.
    /// </summary>
    internal async Task<bool> CreateAsync(
        DatabaseDescriptor database, CreateSequenceTicket ticket, CancellationToken cancellationToken)
    {
        if (ticket.IfNotExists && CatalogsManager.SequenceExists(database, ticket.SequenceName))
            return false;

        SequenceSchema sequence = await CreateOwnedAsync(
            database,
            ticket.SequenceName,
            ticket.StartValue,
            ticket.Increment,
            ticket.MinValue,
            ticket.MaxValue,
            ticket.CacheSize,
            ownedByTableId: null,
            cancellationToken).ConfigureAwait(false);

        return sequence is not null;
    }

    /// <summary>
    /// Creates a sequence, optionally owned by a column. Shared by <c>CREATE SEQUENCE</c> and by
    /// the <c>SERIAL</c> / <c>GENERATED AS IDENTITY</c> desugaring, so both produce exactly the same
    /// object and the same crash-window behavior.
    /// </summary>
    /// <returns>The record as it was proposed, including the id the caller needs to bind a column default to.</returns>
    internal async Task<SequenceSchema> CreateOwnedAsync(
        DatabaseDescriptor database,
        string sequenceName,
        long startValue,
        long increment,
        long minValue,
        long? maxValue,
        int? cacheSize,
        string? ownedByTableId,
        CancellationToken cancellationToken)
    {
        // Allocated before the delta so every node records the id the proposer chose, exactly as
        // table and view creation do. Sequences share that counter with relations, so a sequence and
        // a table can never collide in the metadata keyspace.
        DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);
        string sequenceId = await registry.AllocateTableIdAsync().ConfigureAwait(false);

        SequenceSchema sequence = new()
        {
            Id = sequenceId,
            Name = sequenceName,
            StartValue = startValue,
            Increment = increment,
            MinValue = minValue,
            MaxValue = maxValue,
            CacheSize = cacheSize,
            OwnedByTableId = ownedByTableId,
        };

        await catalogs.CreateSequenceAsync(database, sequence).ConfigureAwait(false);

        // The record is committed and replicated; the counter follows. See the class summary for
        // why this order, and what repairs a crash between the two.
        await allocator.CreateAsync(database.Kahuna.Kahuna, database.Id, sequence, cancellationToken).ConfigureAwait(false);

        return sequence;
    }

    /// <summary>
    /// Drops a sequence. Returns false only when <c>IF EXISTS</c> was written and the sequence is
    /// absent.
    /// </summary>
    internal async Task<bool> DropAsync(
        DatabaseDescriptor database, DropSequenceTicket ticket, CancellationToken cancellationToken)
    {
        if (!database.Schema.Sequences.TryGetValue(ticket.SequenceName, out SequenceSchema? sequence))
        {
            if (ticket.IfExists)
                return false;

            throw new CamusDBException(
                CamusDBErrorCodes.SequenceDoesntExist, $"Sequence '{ticket.SequenceName}' does not exist");
        }

        RequireDroppable(database, sequence);

        await DropByNameAsync(database, ticket.SequenceName, cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Removes the record and then the counter, with no dependency checks. The caller is the one
    /// that decided the sequence may go — a user's <c>DROP SEQUENCE</c> after
    /// <see cref="RequireDroppable"/>, or a <c>DROP TABLE</c> taking its owned sequences with it.
    /// </summary>
    internal async Task DropByNameAsync(
        DatabaseDescriptor database, string sequenceName, CancellationToken cancellationToken)
    {
        string sequenceId = await catalogs.DropSequenceAsync(database, sequenceName).ConfigureAwait(false);

        await allocator.DeleteAsync(database.Kahuna.Kahuna, database.Id, sequenceId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Renames a sequence. Metadata-only: the counter is named after the id, which does not change.</summary>
    internal async Task RenameAsync(DatabaseDescriptor database, RenameSequenceTicket ticket)
        => await catalogs.RenameSequenceAsync(database, ticket.SequenceName, ticket.NewName).ConfigureAwait(false);

    /// <summary>
    /// Applies an <c>ALTER SEQUENCE</c>: the counter first, then the catalog record.
    /// </summary>
    internal async Task AlterAsync(
        DatabaseDescriptor database, AlterSequenceTicket ticket, CancellationToken cancellationToken)
    {
        if (!database.Schema.Sequences.TryGetValue(ticket.SequenceName, out SequenceSchema? sequence))
            throw new CamusDBException(
                CamusDBErrorCodes.SequenceDoesntExist, $"Sequence '{ticket.SequenceName}' does not exist");

        // The folded definition — the recorded values with the statement's changes applied — is what
        // must hold, not the statement in isolation. Lowering MAXVALUE below a MINVALUE the
        // statement never mentioned is exactly the case a ticket-only check cannot see.
        long increment = ticket.Increment ?? sequence.Increment;
        long minValue = ticket.RemoveMinValue ? long.MinValue + increment : ticket.MinValue ?? sequence.MinValue;
        long? maxValue = ticket.RemoveMaxValue ? null : ticket.MaxValue ?? sequence.MaxValue;
        long startValue = ticket.StartValue ?? sequence.StartValue;

        // The whole folded definition, held to exactly the rules CREATE is held to — including the
        // start value, which an ALTER that raises MINVALUE or lowers MAXVALUE can leave outside its
        // own bounds, and the underflow guard that keeps the counter's seed from wrapping.
        try
        {
            CommandsValidator.Validators.SequenceValidator.RequireDefinitionHolds(
                minValue, maxValue, startValue, increment);
        }
        catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.InvalidSequenceDefinition)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidSequenceDefinition,
                $"ALTER SEQUENCE '{ticket.SequenceName}' would leave a definition that cannot hold: {ex.Message}");
        }

        long? restartTo = null;

        if (ticket.Restart)
        {
            restartTo = ticket.RestartWith ?? startValue;

            if (restartTo < minValue)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidSequenceDefinition,
                    $"RESTART WITH ({restartTo}) is below MINVALUE ({minValue}) for sequence '{ticket.SequenceName}'");

            if (maxValue is { } restartCeiling && restartTo > restartCeiling)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidSequenceDefinition,
                    $"RESTART WITH ({restartTo}) is above MAXVALUE ({restartCeiling}) for sequence '{ticket.SequenceName}'");

            // The counter is seeded one increment below the target, so a target near the smallest
            // 64-bit integer would wrap that subtraction into a huge positive seed — a sequence
            // that silently restarts near the top of the range instead of where it was asked to.
            if (restartTo - increment > restartTo)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidSequenceDefinition,
                    $"RESTART WITH ({restartTo}) is too close to the smallest 64-bit integer to seed the " +
                    $"counter for sequence '{ticket.SequenceName}'; it must be at least {long.MinValue} + " +
                    $"INCREMENT ({increment})");
        }

        // The counter first: it is the side that can be refused or can take a lease period to
        // answer, so a failure here leaves the record still describing what the counter is.
        await allocator.UpdateAsync(
            database.Kahuna.Kahuna,
            database.Id,
            sequence,
            // The counter holds the reserved ceiling, so seeding it one increment below the target
            // makes the next value issued be the target itself.
            currentValue: restartTo is { } target ? target - increment : null,
            increment: ticket.Increment,
            initialValue: ticket.StartValue,
            maxValue: ticket.RemoveMaxValue ? null : ticket.MaxValue,
            removeMaxValue: ticket.RemoveMaxValue,
            blockSize: ticket.RemoveCacheSize ? null : ticket.CacheSize,
            removeBlockSize: ticket.RemoveCacheSize,
            cancellationToken).ConfigureAwait(false);

        await catalogs.AlterSequenceAsync(database, new SchemaAlterSequencePayload
        {
            SequenceId = sequence.Id!,
            StartValue = ticket.StartValue,
            Increment = ticket.Increment,
            MinValue = ticket.RemoveMinValue ? long.MinValue + increment : ticket.MinValue,
            MaxValue = ticket.RemoveMaxValue ? null : ticket.MaxValue,
            RemoveMaxValue = ticket.RemoveMaxValue,
            CacheSize = ticket.RemoveCacheSize ? null : ticket.CacheSize,
            RemoveCacheSize = ticket.RemoveCacheSize,
            SetOwner = false,
        }).ConfigureAwait(false);
    }

    /// <summary>
    /// Attaches or removes a sequence's comment. A null <paramref name="comment"/> removes it; the
    /// empty string stores a present-but-empty one, and the two stay distinguishable.
    /// </summary>
    /// <remarks>
    /// Rides the <c>AlterSequence</c> delta rather than the relation comment machinery, which is
    /// keyed on a table and its columns and indexes. A sequence is none of those, so extending that
    /// op would mean teaching it about an object it otherwise has nothing to do with.
    /// </remarks>
    internal async Task SetCommentAsync(DatabaseDescriptor database, string sequenceName, string? comment)
    {
        if (!database.Schema.Sequences.TryGetValue(sequenceName, out SequenceSchema? sequence))
            throw new CamusDBException(
                CamusDBErrorCodes.SequenceDoesntExist, $"Sequence '{sequenceName}' does not exist");

        // The same ceiling every other COMMENT ON target is held to. Checked here because this
        // statement builds no CommentTicket and so never reaches the shared comment validator.
        if (comment is not null && comment.Length > CamusDBConstants.MaxCommentLength)
            throw new CamusDBException(
                CamusDBErrorCodes.CommentTooLong,
                $"Sequence comment is {comment.Length} characters, exceeding the maximum of " +
                $"{CamusDBConstants.MaxCommentLength}");

        await catalogs.AlterSequenceAsync(database, new SchemaAlterSequencePayload
        {
            SequenceId = sequence.Id!,
            Comment = comment,
            SetComment = true,
        }).ConfigureAwait(false);
    }

    /// <summary>
    /// Moves a sequence's counter so the next value it issues is <paramref name="nextValue"/>, and
    /// records nothing. Used by <c>setval</c> and by <c>TRUNCATE … RESTART IDENTITY</c>, where the
    /// recorded parameters do not change — only the position does.
    /// </summary>
    internal async Task SetCounterAsync(
        DatabaseDescriptor database, SequenceSchema sequence, long nextValue, CancellationToken cancellationToken)
        => await allocator.UpdateAsync(
            database.Kahuna.Kahuna,
            database.Id,
            sequence,
            currentValue: nextValue - sequence.Increment,
            increment: null,
            initialValue: null,
            maxValue: null,
            removeMaxValue: false,
            blockSize: null,
            removeBlockSize: false,
            cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Refuses a bare <c>DROP SEQUENCE</c> that would break something still using the sequence.
    /// </summary>
    /// <remarks>
    /// Two distinct cases, and they need distinct messages. An <b>owned</b> sequence belongs to an
    /// identity column and goes when that column goes, so naming it directly is a mistake about
    /// which object to drop. An <b>unowned</b> sequence that a column merely defaults from is a
    /// shared counter, and dropping it would leave the next insert into that table failing on a
    /// counter that is gone.
    ///
    /// <para>The check runs in the caller's flow, not under the schema lock, and is therefore a
    /// check-then-act against a concurrent <c>ALTER TABLE … SET DEFAULT</c>. That race loses at
    /// worst a default pointing at a dropped sequence, which the next insert reports and an
    /// <c>ALTER TABLE</c> repairs — the same exposure a concurrent <c>DROP TABLE</c> has always
    /// had here.</para>
    /// </remarks>
    private static void RequireDroppable(DatabaseDescriptor database, SequenceSchema sequence)
    {
        if (sequence.OwnedByTableId is { Length: > 0 } ownerTableId)
        {
            string ownerName = database.Schema.Tables.Values
                .FirstOrDefault(table => string.Equals(table.Id, ownerTableId, StringComparison.Ordinal))?.Name
                ?? ownerTableId;

            throw new CamusDBException(
                CamusDBErrorCodes.SequenceInUse,
                $"Sequence '{sequence.Name}' is owned by an identity column of '{ownerName}' and cannot be " +
                "dropped on its own. Drop the column or the table instead.");
        }

        List<string> dependents = [];

        foreach (TableSchema table in database.Schema.Tables.Values)
        {
            if (table.Columns is null)
                continue;

            foreach (TableColumnSchema column in table.Columns)
            {
                if (string.Equals(column.DefaultSequenceId, sequence.Id, StringComparison.Ordinal))
                    dependents.Add($"{table.Name}.{column.Name}");
            }
        }

        if (dependents.Count > 0)
            throw new CamusDBException(
                CamusDBErrorCodes.SequenceInUse,
                $"Cannot drop sequence '{sequence.Name}' because these column defaults draw from it: " +
                $"{string.Join(", ", dependents)}. Remove the defaults first.");
    }
}
