/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Catalogs.Apply;

/// <summary>
/// Applies the committed deltas that create, drop, rename and alter a user sequence.
///
/// <para><b>Only the catalog record moves here.</b> The counter lives in Kahuna's sequencer, which
/// is storage; apply runs inside the schema partition's commit pipeline, and storage work from
/// there re-enters the same partition and deadlocks it. The proposer creates, updates and deletes
/// the counter around the replication instead, and a first <c>nextval</c> that finds no counter
/// creates one from the record. That healing path is what makes the crash window benign: a create
/// that replicated and then died leaves a record naming a counter that does not exist yet, which
/// the next use repairs, rather than a counter nothing names.</para>
///
/// <para><b>Every arm is idempotent.</b> Raft re-delivers and the WAL replays, so a second delivery
/// must be a no-op and never an exception — an exception here wedges every later delta for the
/// database.</para>
///
/// <para>Each arm returns null: a sequence is not a relation, so there is no
/// <see cref="TableSchema"/> for the checkpoint to persist. The proposer writes the sequence's own
/// meta key afterwards.</para>
/// </summary>
internal static class SequenceDeltaApplier
{
    /// <summary>
    /// Applies a CreateSequence delta. A record already present under the same id is a re-delivery
    /// and is left alone; a record present under the same name with a <b>different</b> id is a real
    /// name conflict the validator should have caught, and is refused.
    /// </summary>
    internal static TableSchema? ApplyCreateSequence(Schema schema, SchemaSequencePayload payload)
    {
        if (schema.Sequences.TryGetValue(payload.SequenceName, out SequenceSchema? existing))
        {
            if (string.Equals(existing.Id, payload.SequenceId, StringComparison.Ordinal))
                return null;

            throw new CamusDBException(
                CamusDBErrorCodes.SequenceAlreadyExists,
                $"Relation '{payload.SequenceName}' already exists");
        }

        // A name held by a table or a view is not ours to take. Sequences share one namespace with
        // relations, so this is the same check CREATE TABLE and CREATE VIEW make.
        if (schema.Tables.ContainsKey(payload.SequenceName) || schema.Views.ContainsKey(payload.SequenceName))
            throw new CamusDBException(
                CamusDBErrorCodes.TableAlreadyExists,
                $"Relation '{payload.SequenceName}' already exists");

        schema.Sequences[payload.SequenceName] = new SequenceSchema
        {
            Id = payload.SequenceId,
            Name = payload.SequenceName,
            StartValue = payload.StartValue,
            Increment = payload.Increment,
            MinValue = payload.MinValue,
            MaxValue = payload.MaxValue,
            CacheSize = payload.CacheSize,
            OwnedByTableId = payload.OwnedByTableId,
            Comment = payload.Comment,
        };

        return null;
    }

    /// <summary>
    /// Applies a DropSequence delta. A sequence that is already gone is a no-op rather than a
    /// failure, so a re-delivered entry — or one that arrives after a later, already-applied entry
    /// removed the sequence — cannot wedge the apply pipeline.
    /// </summary>
    internal static TableSchema? ApplyDropSequence(Schema schema, SchemaDropSequencePayload payload)
    {
        // Matched on the id when the payload carries one: a name that was freed and re-used by a
        // different sequence must not be removed by this entry's replay.
        if (payload.SequenceId is { Length: > 0 } droppedId
            && schema.Sequences.TryGetValue(payload.SequenceName, out SequenceSchema? live)
            && !string.Equals(live.Id, droppedId, StringComparison.Ordinal))
            return null;

        schema.Sequences.Remove(payload.SequenceName);
        return null;
    }

    /// <summary>
    /// Applies a RenameSequence delta by swapping the map key. The id is deliberately unchanged, so
    /// the Kahuna counter stays where it is and every column default bound to that id keeps
    /// resolving.
    /// </summary>
    internal static TableSchema? ApplyRenameSequence(Schema schema, SchemaRenamePayload payload)
    {
        if (!schema.Sequences.TryGetValue(payload.TableName, out SequenceSchema? sequence))
        {
            // Already renamed by an earlier delivery of this same entry.
            if (schema.Sequences.ContainsKey(payload.NewName))
                return null;

            throw new CamusDBException(
                CamusDBErrorCodes.SequenceDoesntExist,
                $"Sequence '{payload.TableName}' does not exist");
        }

        if (schema.Tables.ContainsKey(payload.NewName) || schema.Views.ContainsKey(payload.NewName)
            || schema.Sequences.ContainsKey(payload.NewName))
            throw new CamusDBException(
                CamusDBErrorCodes.SequenceAlreadyExists,
                $"Relation '{payload.NewName}' already exists");

        schema.Sequences.Remove(payload.TableName);
        sequence.Name = payload.NewName;
        schema.Sequences[payload.NewName] = sequence;

        return null;
    }

    /// <summary>
    /// Applies an AlterSequence delta: the parameters the statement named, and nothing else.
    /// Idempotent — writing the same values twice is the same state — and a no-op for a sequence
    /// that no longer exists, so an entry re-delivered after a DROP cannot wedge apply.
    /// </summary>
    internal static TableSchema? ApplyAlterSequence(Schema schema, SchemaAlterSequencePayload payload)
    {
        SequenceSchema? sequence = schema.FindSequenceById(payload.SequenceId);

        if (sequence is null)
            return null;

        if (payload.StartValue is { } startValue)
            sequence.StartValue = startValue;

        if (payload.Increment is { } increment)
            sequence.Increment = increment;

        if (payload.MinValue is { } minValue)
            sequence.MinValue = minValue;

        // The value and the removal flag are separate because the field is nullable at rest: a null
        // on its own cannot say whether the statement wrote NO MAXVALUE or did not mention it.
        if (payload.RemoveMaxValue)
            sequence.MaxValue = null;
        else if (payload.MaxValue is { } maxValue)
            sequence.MaxValue = maxValue;

        if (payload.RemoveCacheSize)
            sequence.CacheSize = null;
        else if (payload.CacheSize is { } cacheSize)
            sequence.CacheSize = cacheSize;

        if (payload.SetOwner)
            sequence.OwnedByTableId = payload.OwnedByTableId;

        if (payload.SetComment)
            sequence.Comment = payload.Comment;

        return null;
    }
}
