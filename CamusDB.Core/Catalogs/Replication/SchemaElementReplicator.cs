
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.Catalogs.Replication;

/// <summary>
/// Proposes a change to one element of a relation — a column, an index, a constraint, a comment, a
/// settings block — and does not return until the cluster has applied it.
///
/// <para><b>Every method here follows the same three beats, and the order is the point:</b> take the
/// schema lock, build and validate the delta against the current version, <b>release the lock</b>,
/// and only then hand the entry to <see cref="SchemaChangePublisher"/>. Replication re-enters the
/// schema partition's apply pipeline, which yields on that same lock — proposing while holding it
/// deadlocks the partition. The lock covers the read-then-build window so the version the entry
/// claims to follow is still the live one; it must not cover the round-trip.</para>
///
/// <para><b>The delta is validated inside the lock, by dry-running it against a clone of the
/// schema.</b> A delta that cannot apply is rejected here, where the caller still gets an error,
/// rather than at apply time on every node, where a throw would wedge the serial pipeline for that
/// database.</para>
///
/// <para>The staged operations (<see cref="ReplicateAddColumnInStateAsync"/>,
/// <see cref="ReplicateAddIndexInStateAsync"/>, <see cref="ReplicateElementStateAsync"/>) exist so a
/// new element can be introduced through <c>Absent -> DeleteOnly -> WriteOnly -> Public</c> instead
/// of appearing everywhere at once. Backfilling the element's data happens between those states, in
/// committed steps the coordinator drives — never from here.</para>
/// </summary>
internal sealed class SchemaElementReplicator
{
    private readonly SchemaChangePublisher publisher;

    public SchemaElementReplicator(SchemaChangePublisher publisher)
    {
        // Captured, not read per call: a wrong construction order must fail at startup rather than
        // as a null reference in the middle of a DDL statement.
        ArgumentNullException.ThrowIfNull(publisher);

        this.publisher = publisher;
    }

    /// <summary>
    /// Replicates a completed AddIndex or DropIndex change to all cluster nodes via the
    /// schema log. Must be called AFTER the local work (backfill etc.) is done and
    /// <c>table.Schema.Indexes</c> reflects the final state. Only called when
    /// <c>isClusterMode</c>; standalone nodes need no replication.
    /// </summary>
    internal async Task ReplicateIndexChangeAsync(
        DatabaseDescriptor database,
        AlterIndexTicket ticket,
        TableDescriptor table,
        KvTransaction tx
    )
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            entry = ticket.Operation is AlterIndexOperation.DropIndex or AlterIndexOperation.DropPrimaryKey
                ? SchemaChangeEntryFactory.DropIndexEntry(database, ticket, tx)
                : SchemaChangeEntryFactory.AddIndexEntry(database, ticket, table, tx);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    /// <summary>
    /// Proposes an <c>AddColumn</c> delta with <paramref name="initialState"/> and replicates
    /// it to all cluster nodes via the schema log.  Used by
    /// <see cref="SchemaChangeCoordinator"/> to begin a staged add sequence in
    /// <c>DeleteOnly</c> rather than jumping straight to <c>Public</c>.
    /// Only valid on cluster nodes (<c>isClusterMode</c>).
    /// </summary>
    internal async Task ReplicateAddColumnInStateAsync(
        DatabaseDescriptor database,
        string tableName,
        ColumnInfo column,
        SchemaElementState initialState
    )
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            entry = SchemaChangeEntryFactory.AddColumnInStateEntry(database, tableName, column, initialState);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    /// <summary>
    /// Proposes a single <c>SetElementState</c> delta and replicates it to all cluster nodes
    /// via the schema log, then waits for every live node to ack the resulting version.
    /// Validates the state transition before proposing.
    /// Only valid on cluster nodes (<c>isClusterMode</c>).
    /// </summary>
    internal async Task ReplicateElementStateAsync(
        DatabaseDescriptor database,
        string tableName,
        string elementName,
        SchemaElementState targetState,
        SchemaElementKind elementKind = SchemaElementKind.Column
    )
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            entry = SchemaChangeEntryFactory.ElementStateEntry(database, tableName, elementName, targetState, elementKind);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    /// <summary>
    /// Proposes an <c>AddIndex</c> delta with <paramref name="initialState"/> and replicates
    /// it to all cluster nodes via the schema log. Used by <see cref="SchemaChangeCoordinator"/>
    /// to begin the staged add sequence in <c>DeleteOnly</c> rather than jumping straight to
    /// <c>Public</c>. Only valid on cluster nodes (<c>isClusterMode</c>).
    /// </summary>
    internal async Task ReplicateAddIndexInStateAsync(
        DatabaseDescriptor database,
        string tableName,
        IndexBuildInfo indexBuildInfo,
        SchemaElementState initialState
    )
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            entry = SchemaChangeEntryFactory.AddIndexInStateEntry(database, tableName, indexBuildInfo, initialState);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    /// <summary>
    /// Proposes a <c>DropIndex</c> delta and replicates it to all cluster nodes.
    /// Used to compensate a failed coordinator-driven add-index sequence: if the
    /// index was added in <c>DeleteOnly</c> or <c>WriteOnly</c> state but the sequence
    /// did not reach <c>Public</c>, this removes it cleanly on every node.
    /// Only valid on cluster nodes (<c>isClusterMode</c>). No-op if the index is
    /// already absent (idempotent).
    /// </summary>
    internal async Task ReplicateDropIndexAsync(DatabaseDescriptor database, string tableName, string indexName)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            entry = SchemaChangeEntryFactory.DropIndexByNameEntry(database, tableName, indexName);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    /// <summary>
    /// Proposes a <see cref="SchemaOp.SetTableSettings"/> delta and replicates it to all cluster nodes,
    /// so every node's in-memory <see cref="TableSchema.Settings"/> updates and the KV checkpoint is
    /// rewritten. Advances the database schema version (like check constraints) but not
    /// <see cref="TableSchema.Version"/> — settings do not affect row encoding.
    ///
    /// <para><b>Both modes use this path.</b> A standalone node is the only member of its own schema
    /// group, so proposing costs it a single-node commit and buys the same guarantees the cluster gets:
    /// the database schema version and the descriptor's head-version fence advance together, from the
    /// one place that advances them for every other schema operation. The earlier standalone shortcut
    /// wrote the table blob directly and left the version untouched, which made a background sweep that
    /// keys on the database schema version unable to notice a table's settings changing at all — TTL
    /// could be switched on and the sweep would never see it. Changing settings <em>is</em> a schema
    /// change; do not reintroduce a mode-specific path that pretends otherwise.</para>
    /// </summary>
    internal async Task ReplicateSetTableSettingsAsync(
        DatabaseDescriptor database,
        string tableName,
        IReadOnlyDictionary<string, string> settings,
        IReadOnlyCollection<string>? removedKeys = null)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            entry = SchemaChangeEntryFactory.SetTableSettingsEntry(database, tableName, settings, removedKeys);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    internal async Task ReplicateSetCommentAsync(
        DatabaseDescriptor database,
        string tableName,
        CommentTarget target,
        string? elementName,
        string? comment)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            entry = SchemaChangeEntryFactory.SetCommentEntry(database, tableName, target, elementName, comment);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    /// <summary>
    /// Proposes an <c>AddCheckConstraint</c> delta and replicates it to all cluster nodes.
    /// The proposer must already have validated the expression and confirmed existing rows pass.
    /// Only valid when <c>isClusterMode</c>; standalone nodes apply the change directly.
    /// </summary>
    internal async Task ReplicateAddCheckConstraintAsync(
        DatabaseDescriptor database,
        string tableName,
        string constraintName,
        string expression,
        string[] referencedColumns)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            entry = SchemaChangeEntryFactory.AddCheckConstraintEntry(database, tableName, constraintName, expression, referencedColumns);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    /// <summary>
    /// Proposes a <c>DropCheckConstraint</c> delta and replicates it to all cluster nodes.
    /// Idempotent: if the constraint is already absent, the delta is a no-op but still advances
    /// the schema version.
    /// Only valid when <c>isClusterMode</c>; standalone nodes apply the change directly.
    /// </summary>
    internal async Task ReplicateDropCheckConstraintAsync(
        DatabaseDescriptor database,
        string tableName,
        string constraintName)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            entry = SchemaChangeEntryFactory.DropCheckConstraintEntry(database, tableName, constraintName);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    /// <summary>
    /// Proposes a <c>SetColumnNotNull</c> delta and replicates it to all cluster nodes.
    /// Idempotent: if the column's NOT NULL state already matches, the delta is a no-op but still
    /// advances the schema version.
    /// Only valid when <c>isClusterMode</c>; standalone nodes apply the change directly.
    /// </summary>
    internal async Task ReplicateSetColumnNotNullAsync(
        DatabaseDescriptor database,
        string tableName,
        string columnName,
        bool notNull,
        string? constraintName)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            entry = SchemaChangeEntryFactory.SetColumnNotNullEntry(database, tableName, columnName, notNull, constraintName);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }
}
