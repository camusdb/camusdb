/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Catalogs.Replication;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Catalogs;

/// <summary>
/// Creates, drops, renames and alters the <b>catalog record</b> of a user sequence. Every
/// operation is a replicated schema change: it builds a delta, validates it under the schema lock,
/// releases the lock, and hands the delta to <see cref="SchemaChangePublisher"/>.
///
/// <para><b>The Kahuna counter is not touched here.</b> This class owns the replicated record
/// only. The counter is created, updated and deleted by <c>SequenceDdlService</c> around these
/// calls, because apply runs inside the schema partition's commit pipeline and storage work from
/// there deadlocks it. Splitting the two is what forces the ordering question to be answered
/// explicitly rather than by accident — see <c>SequenceDdlService</c> for which side goes first
/// and why.</para>
///
/// <para><b>Name availability is checked here, under the lock, in the same critical section that
/// builds the delta.</b> Checking it in the caller would be a check-then-act that two concurrent
/// <c>CREATE SEQUENCE</c> statements both pass.</para>
/// </summary>
internal sealed class SequenceCatalog
{
    private readonly SchemaChangePublisher publisher;

    public SequenceCatalog(SchemaChangePublisher publisher)
    {
        ArgumentNullException.ThrowIfNull(publisher);

        this.publisher = publisher;
    }

    /// <summary>Whether a sequence with this name currently exists.</summary>
    internal static bool SequenceExists(DatabaseDescriptor database, string sequenceName)
        => database.Schema.Sequences.ContainsKey(sequenceName);

    /// <summary>
    /// Proposes a <see cref="SchemaOp.CreateSequence"/> delta and waits for it to apply locally. The
    /// id is allocated by the caller and carried in the payload so every node records the same one.
    /// </summary>
    internal async Task CreateSequenceAsync(DatabaseDescriptor database, SequenceSchema sequence)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            database.Schema.RequireRelationNameAvailable(sequence.Name!);

            entry = SchemaChangeEntryFactory.CreateSequenceEntry(database, sequence);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    /// <summary>
    /// Proposes a <see cref="SchemaOp.DropSequence"/> delta and waits for it to apply locally.
    /// Returns the id of the sequence that was dropped, so the caller can delete its counter — the
    /// record is gone from memory by the time this returns.
    /// </summary>
    internal async Task<string> DropSequenceAsync(DatabaseDescriptor database, string sequenceName)
    {
        SchemaChangeLogEntry entry;
        string sequenceId;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            if (!database.Schema.Sequences.TryGetValue(sequenceName, out SequenceSchema? sequence))
                throw new CamusDBException(
                    CamusDBErrorCodes.SequenceDoesntExist, $"Sequence '{sequenceName}' does not exist");

            sequenceId = sequence.Id!;

            entry = SchemaChangeEntryFactory.DropSequenceEntry(database, sequenceName, sequenceId);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);

        return sequenceId;
    }

    /// <summary>Proposes a <see cref="SchemaOp.RenameSequence"/> delta and waits for it to apply locally.</summary>
    internal async Task RenameSequenceAsync(DatabaseDescriptor database, string sequenceName, string newName)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            if (!database.Schema.Sequences.ContainsKey(sequenceName))
                throw new CamusDBException(
                    CamusDBErrorCodes.SequenceDoesntExist, $"Sequence '{sequenceName}' does not exist");

            database.Schema.RequireRelationNameAvailable(newName);

            entry = SchemaChangeEntryFactory.RenameSequenceEntry(database, sequenceName, newName);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    /// <summary>
    /// Proposes a <see cref="SchemaOp.AlterSequence"/> delta carrying only the fields the statement
    /// named, and waits for it to apply locally.
    /// </summary>
    internal async Task AlterSequenceAsync(DatabaseDescriptor database, SchemaAlterSequencePayload payload)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            if (database.Schema.FindSequenceById(payload.SequenceId) is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.SequenceDoesntExist, $"Sequence '{payload.SequenceId}' does not exist");

            entry = SchemaChangeEntryFactory.AlterSequenceEntry(database, payload);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }
}
