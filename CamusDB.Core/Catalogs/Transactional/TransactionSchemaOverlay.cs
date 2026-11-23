
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Transactions;
using Microsoft.Extensions.Logging;
using Nito.AsyncEx;

namespace CamusDB.Core.Catalogs.Transactional;

/// <summary>
/// The private schema of one explicit transaction that ran schema DDL with transactional DDL on: a copy
/// of the shared schema taken at the transaction's first schema change, plus the change set — the deltas
/// its DDL statements produced, in order. A table is copied right before the first delta that changes it;
/// until then the private schema holds the shared object, and no statement may change it in place.
///
/// <para><b>Only the owner transaction reads it.</b> While a statement of the owner runs,
/// <see cref="TransactionSchemaScope"/> makes <see cref="DatabaseDescriptor.Schema"/>,
/// <see cref="DatabaseDescriptor.TableDescriptors"/>, <see cref="DatabaseDescriptor.SystemSchema"/> and
/// <see cref="DatabaseDescriptor.BoundQueries"/> answer from here, and turns the result cache off. Every
/// other transaction, on every node, sees the shared schema and nothing of the change set until the
/// commit publishes it.</para>
///
/// <para><b>A DDL statement runs its normal code.</b> It validates against this schema, builds its delta
/// with the same factory autocommit DDL uses, and calls the publisher, which hands the delta to
/// <see cref="ApplyStatementDeltaAsync"/> instead of the schema log. Nothing it does reaches the shared
/// schema. Its catalog writes inside the transaction are skipped (<see cref="AllowsCatalogWrites"/>):
/// the publish checkpoint writes them, and a version key written here would move the persisted schema
/// version past the schema log.</para>
///
/// <para><b>Rows the transaction writes use this schema's layouts.</b> A table layout version is counted
/// per table, so the publish produces the same version numbers with the same layouts. The commit writes
/// the history key of every new layout in the transaction itself, so a node that meets one of these rows
/// before it applies the publish loads the layout by version.</para>
///
/// <para>The commit protocol is <see cref="ChangeSetCommitter"/>'s; this type holds the state.</para>
/// </summary>
internal sealed class TransactionSchemaOverlay : ITransactionSchemaChanges
{
    private enum OverlayState
    {
        /// <summary>DDL statements may still add deltas.</summary>
        Open,

        /// <summary>The commit started; the change set is fixed.</summary>
        Preparing,

        /// <summary>The schema log holds the change set as pending; a publish or an abort must end it.</summary>
        Pending,

        /// <summary>The transaction ended. Lookups fall back to the shared schema.</summary>
        Closed
    }

    private readonly KvTransaction owner;

    private readonly ChangeSetCommitter committer;

    private readonly ILogger logger;

    private readonly TransactionalDdlLock ddlLock;

    private readonly List<SchemaChangeLogEntry> deltas = [];

    private readonly HashSet<string> createdTableIds = new(StringComparer.Ordinal);

    private readonly HashSet<string> touchedTableIds = new(StringComparer.Ordinal);

    /// <summary>
    /// The tables of <see cref="Schema"/> that are the overlay's own: created by the change set, or copied
    /// before their first change. Every other table is the shared object.
    /// </summary>
    private readonly HashSet<TableSchema> ownTables = new(ReferenceEqualityComparer.Instance);

    private readonly List<Func<Task>> afterPublish = [];

    private readonly List<TableSchema> droppedTables = [];

    private readonly List<Func<Task>> onRollback = [];

    private readonly List<(string TableId, string ElementName)> ownedJobs = [];

    private readonly List<DeferredPurgeRecord> deferredPurges = [];

    private OverlayState state = OverlayState.Open;

    private int closed;

    private TransactionSchemaOverlay(
        DatabaseDescriptor database,
        KvTransaction owner,
        Schema schema,
        SystemSchema systemSchema,
        long baseVersion,
        TransactionalDdlLock ddlLock,
        ChangeSetCommitter committer,
        ILogger logger)
    {
        Database = database;
        DatabaseId = database.Id;
        this.owner = owner;
        Schema = schema;
        SystemSchema = systemSchema;
        BaseVersion = baseVersion;
        this.ddlLock = ddlLock;
        this.committer = committer;
        this.logger = logger;
    }

    /// <summary>The database the change set belongs to. One transaction changes the schema of one database.</summary>
    internal DatabaseDescriptor Database { get; }

    internal string DatabaseId { get; }

    /// <summary>The private schema: the shared schema at <see cref="BaseVersion"/> with the change set applied.</summary>
    internal Schema Schema { get; }

    /// <summary>A private copy of the legacy system schema, which table creation and drop also edit.</summary>
    internal SystemSchema SystemSchema { get; }

    /// <summary>
    /// The table descriptors the owner opens, built from <see cref="Schema"/>. Never shared, and cleared
    /// after each delta so the next open builds from the changed schema.
    /// </summary>
    internal ConcurrentDictionary<string, AsyncLazy<TableDescriptor>> TableDescriptors { get; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>A private bound-query cache, so no bound statement of the owner reaches the shared one.</summary>
    internal BoundQueryCache BoundQueries { get; } = new();

    /// <summary>
    /// The shared schema version the private copy is based on: the version at the transaction's first
    /// schema change, moved forward by <see cref="RebaseOntoSharedTableAsync"/> after the staged steps of a
    /// statement on a shared table.
    /// </summary>
    internal long BaseVersion { get; private set; }

    /// <summary>The id that names the change set's decision key and pending record. Fresh per transaction.</summary>
    internal string ChangeSetId { get; } = Guid.NewGuid().ToString("N");

    /// <summary>True until the transaction ends; a closed overlay redirects nothing.</summary>
    internal bool IsOpen => Volatile.Read(ref closed) == 0;

    /// <summary>
    /// True only while the commit writes the decision key and the layouts. Every other catalog write the
    /// owner makes is skipped; see the class summary.
    /// </summary>
    internal bool AllowsCatalogWrites { get; set; }

    /// <summary>The deltas of the change set, in the order the statements produced them.</summary>
    internal IReadOnlyList<SchemaChangeLogEntry> Deltas => deltas;

    /// <summary>
    /// The tables the change set dropped, as they were right before the drop. Their rows stay on disk under
    /// an orphan record, so the commit writes their new layouts as it does for the tables that remain.
    /// </summary>
    internal IReadOnlyList<TableSchema> DroppedTables => droppedTables;

    /// <summary>The ids of the tables the change set changes, for the cache invalidation after the publish.</summary>
    internal IReadOnlyCollection<string> TouchedTableIds => touchedTableIds;

    internal KvTransaction Owner => owner;

    /// <summary>The committer of this overlay; tests reach its fault hook through it.</summary>
    internal ChangeSetCommitter Committer => committer;

    public bool HasChanges => deltas.Count > 0;

    /// <summary>
    /// The coordinator job records of the elements this transaction staged on shared tables, by table id
    /// and element name (<see cref="AddOwnedJob"/>). The commit deletes them in the transaction's own KV
    /// commit, so a record that names this change set as its owner exists only while the transaction did
    /// not commit.
    /// </summary>
    internal IReadOnlyList<(string TableId, string ElementName)> OwnedJobs => ownedJobs;

    /// <summary>
    /// True when the transaction staged an element on a shared table. Such an overlay must reach the commit
    /// even with no delta: a staged CHECK or NOT NULL is already in the shared schema, and only the commit
    /// makes it permanent.
    /// </summary>
    internal bool HoldsOwnedJobs => ownedJobs.Count > 0;

    /// <summary>Records the job record of an element the transaction staged; see <see cref="OwnedJobs"/>.</summary>
    internal void AddOwnedJob(string tableId, string elementName) => ownedJobs.Add((tableId, elementName));

    /// <summary>
    /// True once a statement recorded the removal of the elements the transaction staged on shared tables.
    /// One removal covers every such element, so it is recorded once.
    /// </summary>
    internal bool OwnedRemovalRecorded { get; set; }

    /// <summary>True when the change set created the table, so only the owner can have written rows to it.</summary>
    internal bool CreatedTable(string tableId) => createdTableIds.Contains(tableId);

    internal bool IsPending => state == OverlayState.Pending;

    /// <summary>
    /// True once a commit attempt of the owner ended with an unknown KV outcome. From then on only a
    /// resumed commit or the resolver may decide the change set: a rollback of the owner must not abort
    /// it, because the KV commit may have landed.
    /// </summary>
    internal bool OutcomeUnknown { get; set; }

    /// <summary>
    /// True while the owner's rows are committed and the publish of the change set is not confirmed. A
    /// retried COMMIT completes it; see <see cref="ChangeSetCommitter"/>.
    /// </summary>
    public bool PublicationOutstanding { get; internal set; }

    /// <summary>
    /// Opens the overlay of <paramref name="tx"/> on <paramref name="database"/>: takes the DDL lock, checks
    /// that no schema change landed under the transaction's earlier statements, and copies the shared
    /// schema. The caller then names the transaction for its flow (<see cref="TransactionSchemaScope.Enter"/>).
    /// </summary>
    internal static async Task<TransactionSchemaOverlay> OpenAsync(
        DatabaseDescriptor database, KvTransaction tx, ChangeSetCommitter committer, ILogger logger)
    {
        TransactionalDdlLock ddlLock = await TransactionalDdlLock.AcquireAsync(
            database, TransactionalDdlLock.DefaultWaitTimeout, logger).ConfigureAwait(false);

        try
        {
            // A deferred transaction opens its Kahuna session at its first data operation. A transaction
            // whose first statement is DDL has none yet, and its commit would take the path for a
            // transaction that never started, which commits nothing, change set included.
            await tx.EnsureSessionStartedAsync(CancellationToken.None).ConfigureAwait(false);

            // The statements before this one planned against the shared schema. A change since then
            // would make the copy below a base those statements never saw.
            if (!tx.TryValidateSchemaPins(out string? refusal))
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionConflict,
                    $"The transaction cannot change the schema: {refusal}; a schema change landed after an earlier " +
                    "statement of the transaction; roll back and retry it from BEGIN");

            Schema shared = database.Schema;
            Schema copy;
            long baseVersion;

            await shared.AcquireLockAsync().ConfigureAwait(false);
            try
            {
                if (shared.PendingChangeSet is { } pending)
                    throw new CamusDBException(
                        CamusDBErrorCodes.TransactionMustRetry,
                        $"The schema changes of another transaction (change set {pending.ChangeSetId}) are being " +
                        "published; retry the statement");

                // The tables are shared until a delta of the transaction changes one. A change of the shared
                // schema by somebody else would show through, and it moves the base version, which refuses
                // the commit.
                copy = SchemaReplicator.CloneSchema(shared, static (_, _) => false);
                baseVersion = shared.SchemaVersion;
            }
            finally
            {
                shared.ReleaseLock();
            }

            copy.PendingChangeSet = null;

            SystemSchema systemCopy;
            await database.SystemSchemaSemaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                systemCopy = new SystemSchema
                {
                    Tables = new(database.SystemSchema.Tables),
                    Indexes = new(database.SystemSchema.Indexes),
                };
            }
            finally
            {
                database.SystemSchemaSemaphore.Release();
            }

            TransactionSchemaOverlay overlay = new(database, tx, copy, systemCopy, baseVersion, ddlLock, committer, logger);

            tx.SchemaChanges = overlay;
            TransactionSchemaScope.OverlayOpened();

            return overlay;
        }
        catch
        {
            await ddlLock.ReleaseAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Applies one delta of a DDL statement to the private schema and adds it to the change set. Called
    /// by the publisher in place of the schema log. Refuses an op that has no transactional form yet,
    /// and raises the refusal of a delta that does not apply, as the schema log would.
    /// </summary>
    internal async Task ApplyStatementDeltaAsync(SchemaChangeLogEntry entry)
    {
        if (state != OverlayState.Open)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"The transaction's schema changes are already being committed; '{entry.Op}' cannot join them");

        RequireTransactionalOp(entry);

        // The private schema shares the tables it did not change with the shared one, so a delta must not
        // build on a base that moved. The commit would refuse the transaction anyway.
        RequireSharedBase();

        List<string> touched = new(2);

        await Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            if (entry.FromVersion != Schema.SchemaVersion)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Schema change '{entry.Op}' was built against version {entry.FromVersion} of the transaction's " +
                    $"schema, which is at version {Schema.SchemaVersion}");

            ChangeSetDeltaApplier.CopyTableOnWrite(Schema, entry, ownTables);

            TableSchema? target = null;
            if (ChangeSetDeltaApplier.TargetTableName(entry) is { } targetName && Schema.Tables.TryGetValue(targetName, out target) && target.Id is { } targetId)
                touched.Add(targetId);

            TableSchema? applied = SchemaDeltaApplier.ApplyOrRefuse(Schema, Database, entry);

            if (Schema.FindRefusal(entry) is { } refusal)
                throw new CamusDBException(refusal.Code, refusal.Message);

            if (applied is not null)
                ownTables.Add(applied);

            if (applied?.Id is { } appliedId)
            {
                touched.Add(appliedId);

                if (entry.Op == SchemaOp.CreateTable)
                    createdTableIds.Add(appliedId);
            }

            deltas.Add(entry);

            // A dropped table's rows stay on disk for the orphan record, so its layouts are still needed.
            if (entry.Op == SchemaOp.DropTable && target is not null)
                droppedTables.Add(target);

            foreach (string tableId in touched)
                touchedTableIds.Add(tableId);
        }
        finally
        {
            Schema.ReleaseLock();
        }

        TableDescriptors.Clear();

        // The owner's own pins on a changed table describe the layout before the change; see
        // KvTransaction.RemoveSchemaPins.
        foreach (string tableId in touched)
            owner.RemoveSchemaPins($"{DatabaseId}/{tableId}");
    }

    /// <summary>
    /// Refuses a schema statement when the shared schema moved since the private copy was based on it: the
    /// copy shares the tables it did not change, and a staged step on a shared table can be taken over only
    /// from the base the copy was built on. The commit would refuse such a transaction anyway.
    /// </summary>
    internal void RequireSharedBase()
    {
        Schema shared = Database.SharedSchema;

        if (shared.SchemaVersion == BaseVersion && shared.PendingChangeSet is null)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.TransactionConflict,
            $"The transaction changed the schema of database '{Database.Name}' from version {BaseVersion}, but the schema " +
            $"is at version {shared.SchemaVersion}: another schema change landed while the transaction was open; roll back " +
            "and retry it from BEGIN");
    }

    /// <summary>
    /// Takes over the staged steps that a statement of the owner ran on the shared table
    /// <paramref name="tableId"/>: the private copy of that table is replaced by a copy of the shared one,
    /// and the base moves to the shared version. <paramref name="stepsTaken"/> is the number of schema
    /// versions the statement's steps took. The private copy of every other table was built on the old
    /// base, so the shared schema must have moved by exactly those steps; otherwise another change landed
    /// in between and the statement fails with <see cref="CamusDBErrorCodes.TransactionConflict"/>.
    ///
    /// <para>The caller checked that the change set has not changed the table, so the shared copy holds
    /// everything the private one held, plus the staged element. The owner's pins on the table are dropped:
    /// they describe the table before the element, and a staged element changes no row.</para>
    ///
    /// <para>The sequences the table owns come with it. An identity column creates its sequence in the
    /// shared schema, and the owner's own inserts resolve the column default through the private one.</para>
    /// </summary>
    internal async Task RebaseOntoSharedTableAsync(string tableId, long stepsTaken)
    {
        Schema shared = Database.SharedSchema;
        TableSchema copy;
        List<SequenceSchema> ownedSequences = [];
        long sharedVersion;

        await shared.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            sharedVersion = shared.SchemaVersion;

            if (sharedVersion != BaseVersion + stepsTaken || shared.PendingChangeSet is not null)
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionConflict,
                    $"Another schema change of database '{Database.Name}' landed while the transaction staged a change on one of " +
                    "its tables; roll back and retry the transaction from BEGIN");

            TableSchema sharedTable = SchemaDeltaApplier.FindRelationById(shared, tableId)
                ?? throw new CamusDBException(
                    CamusDBErrorCodes.TableDoesntExist,
                    $"The table with id '{tableId}' left the schema while the transaction staged a change on it");

            copy = SchemaReplicator.CloneTable(sharedTable);

            foreach (SequenceSchema sequence in shared.Sequences.Values)
            {
                if (string.Equals(sequence.OwnedByTableId, tableId, StringComparison.Ordinal))
                    ownedSequences.Add(sequence.Clone());
            }
        }
        finally
        {
            shared.ReleaseLock();
        }

        await Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            foreach (SequenceSchema sequence in ownedSequences)
                AddOwnedSequence(sequence);

            Schema.Tables[copy.Name!] = copy;
            ownTables.Add(copy);
            Schema.RebuildRelationNameIndex();
            Schema.RebuildSequenceIdIndex();
            Schema.RebuildForeignKeyGraph();

            // The private version stays the base plus one per delta of the change set, which is the
            // version the next statement of the owner builds its delta from.
            Schema.SchemaVersion += sharedVersion - BaseVersion;
            BaseVersion = sharedVersion;

            touchedTableIds.Add(tableId);
        }
        finally
        {
            Schema.ReleaseLock();
        }

        TableDescriptors.Clear();
        owner.RemoveSchemaPins($"{DatabaseId}/{tableId}");
    }

    /// <summary>
    /// Adds a sequence that a staged step created in the shared schema to the private one. A name that the
    /// change set took for another relation fails the statement: the publish of the change set would be
    /// refused for the same reason.
    /// </summary>
    private void AddOwnedSequence(SequenceSchema sequence)
    {
        string name = sequence.Name!;

        if (Schema.Sequences.TryGetValue(name, out SequenceSchema? present)
            && string.Equals(present.Id, sequence.Id, StringComparison.Ordinal))
            return;

        if (present is not null || Schema.Tables.ContainsKey(name) || Schema.Views.ContainsKey(name))
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionConflict,
                $"The sequence '{name}' of the new column has the name of a relation that the transaction created; roll back " +
                "and retry the transaction with another name");

        Schema.Sequences[name] = sequence;
    }

    /// <summary>
    /// Drops the overlay of <paramref name="tx"/> when no statement added a delta to it, and frees the DDL
    /// lock. Called after every successful statement. An overlay with no delta is a private copy of the
    /// shared schema that nothing keeps current: a later statement of the transaction would plan against
    /// it, and its commit would check nothing, because there is no change set to announce. Without it, the
    /// transaction's statements read the shared schema and take their pins on it, as before its first
    /// schema statement. An overlay that holds an element staged on a shared table stays (<see cref="HoldsOwnedJobs"/>).
    /// </summary>
    internal static async Task DiscardIfEmptyAsync(KvTransaction tx)
    {
        if (tx.SchemaChanges is not TransactionSchemaOverlay { IsOpen: true, HasChanges: false, HoldsOwnedJobs: false } overlay)
            return;

        if (overlay.state != OverlayState.Open)
            return;

        tx.SchemaChanges = null;
        await overlay.CloseAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Records work to run after a successful publish. Best effort: it runs once, on this node, and a
    /// failure is logged. Data that must be removed after the publish goes through
    /// <see cref="AddDeferredPurge"/> instead, which is durable.
    /// </summary>
    internal void AddAfterPublish(Func<Task> action) => afterPublish.Add(action);

    /// <summary>
    /// Records data that the change set makes unreachable and that must be removed after its publish, such
    /// as the entries of a dropped index. The commit writes the record in the transaction's own KV commit,
    /// so it survives a crash of this node; see <see cref="DeferredPurgeRecord"/>. A rollback writes nothing
    /// and leaves the data in place.
    /// </summary>
    internal DeferredPurgeRecord AddDeferredPurge(DeferredPurgeKind kind, string tableId, string storageId, IReadOnlyCollection<string> indexIds)
    {
        DeferredPurgeRecord record = new()
        {
            PurgeId = ChangeSetId + "~" + deferredPurges.Count.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Kind = kind,
            ChangeSetId = ChangeSetId,
            BaseVersion = BaseVersion,
            TableId = tableId,
            StorageId = storageId,
            IndexIds = [.. indexIds],
        };

        deferredPurges.Add(record);
        return record;
    }

    /// <summary>
    /// Takes back the newest record of <see cref="AddDeferredPurge"/> when the statement that added it
    /// failed before its delta joined the change set.
    /// </summary>
    internal void RemoveDeferredPurge(DeferredPurgeRecord record) => deferredPurges.Remove(record);

    /// <summary>The records <see cref="AddDeferredPurge"/> collected, in statement order.</summary>
    internal IReadOnlyList<DeferredPurgeRecord> DeferredPurges => deferredPurges;

    /// <summary>
    /// Records work that undoes a side effect a DDL statement could not keep inside the transaction, such
    /// as the Kahuna counter of a sequence the transaction created. Runs when the change set is not
    /// published. Best effort.
    /// </summary>
    internal void AddOnRollback(Func<Task> action) => onRollback.Add(action);

    /// <summary>Set by <see cref="PrepareCommitAsync"/>; it stays set after the overlay closes.</summary>
    public bool IsCommitPrepared { get; private set; }

    internal void MarkPreparing() => state = OverlayState.Preparing;

    internal void MarkPending() => state = OverlayState.Pending;

    /// <summary>Runs the after-publish work. Each action is independent; a failure is logged and the rest still run.</summary>
    internal async Task RunAfterPublishAsync()
    {
        foreach (Func<Task> action in afterPublish)
        {
            try
            {
                await action().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Work after the publish of change set {ChangeSetId} of database {Database} failed", ChangeSetId, Database.Name);
            }
        }
    }

    /// <summary>Runs the rollback work, newest first. A failure is logged and the rest still run.</summary>
    internal async Task RunOnRollbackAsync()
    {
        for (int i = onRollback.Count - 1; i >= 0; i--)
        {
            try
            {
                await onRollback[i]().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Rollback work of change set {ChangeSetId} of database {Database} failed", ChangeSetId, Database.Name);
            }
        }
    }

    /// <summary>
    /// Ends the overlay: lookups fall back to the shared schema, and the DDL lock is freed. Safe to call
    /// twice.
    /// </summary>
    internal async Task CloseAsync()
    {
        if (Interlocked.Exchange(ref closed, 1) != 0)
            return;

        state = OverlayState.Closed;
        TransactionSchemaScope.OverlayClosed();

        await ddlLock.ReleaseAsync().ConfigureAwait(false);
    }

    /// <summary>Test-only: frees the DDL lock while the overlay stays open, as a lapsed lease does.</summary>
    internal Task ReleaseDdlLockForTestsAsync() => ddlLock.ReleaseAsync();

    public bool TryValidateSchemaPins(KvTransaction tx, out string? refusal)
    {
        // Under the private schema, so a pin on a table the transaction created or renamed resolves.
        KvTransaction? previous = TransactionSchemaScope.Swap(tx);
        try
        {
            return tx.TryValidateSchemaPins(out refusal);
        }
        finally
        {
            TransactionSchemaScope.Swap(previous);
        }
    }

    public async Task PrepareCommitAsync(KvTransaction tx, CancellationToken cancellationToken)
    {
        await committer.PrepareAsync(this, cancellationToken).ConfigureAwait(false);
        IsCommitPrepared = true;
    }

    public Task OnCommitOutcomeAsync(KvTransaction tx, SchemaChangesCommitOutcome outcome) =>
        committer.CompleteAsync(this, outcome);

    public Task DiscardAsync(KvTransaction tx) => committer.DiscardAsync(this);

    public Task ReleaseAsync(KvTransaction tx) => CloseAsync();

    public Task ResumePublicationAsync(KvTransaction tx, CancellationToken cancellationToken) =>
        committer.ResumePublicationAsync(this);

    /// <summary>
    /// Refuses a delta whose op has no transactional form yet. The DDL dispatcher refuses the statements
    /// that produce such deltas before they run; this is the check that holds when a statement path
    /// produces one anyway.
    /// </summary>
    /// <summary>True when a <see cref="DeferredPurgeKind.TableData"/> record names the table <paramref name="tableName"/>.</summary>
    private bool PurgesTableData(string tableName) =>
        Schema.Tables.TryGetValue(tableName, out TableSchema? table)
        && deferredPurges.Exists(record => record.Kind == DeferredPurgeKind.TableData && string.Equals(record.TableId, table.Id, StringComparison.Ordinal));

    private void RequireTransactionalOp(SchemaChangeLogEntry entry)
    {
        bool supported = entry.Op switch
        {
            SchemaOp.CreateTable or
            SchemaOp.AddColumn or SchemaOp.DropColumn or
            SchemaOp.RenameTable or SchemaOp.RenameColumn or SchemaOp.RenameIndex or
            SchemaOp.AddIndex or SchemaOp.DropIndex or
            SchemaOp.SetTableSettings or SchemaOp.SetComment or
            SchemaOp.CreateView or SchemaOp.ReplaceView or SchemaOp.DropView or SchemaOp.RenameView or SchemaOp.SetViewDefinition or
            // A sequence enters a change set only as the owned sequence of a column: created with an
            // identity column, dropped with its column. The statements on sequences themselves are refused.
            SchemaOp.CreateSequence or SchemaOp.DropSequence => true,

            // A constraint added inside the transaction: its statement checks the rows the transaction can
            // see before the change set is published.
            SchemaOp.AddCheckConstraint or SchemaOp.SetColumnNotNull or SchemaOp.AddForeignKey => true,

            // A state step joins a change set only to finish an element: a foreign key goes WriteOnly then
            // Public in one statement, so the publish makes it visible enforced and checked. A step back
            // (a removal) has no transactional form.
            SchemaOp.SetElementState => entry.GetPayload<SchemaElementStatePayload>().State == SchemaElementState.Public,

            // A drop keeps the rows and index entries until the publish, so a rollback has nothing to
            // restore: a deferred drop leaves them to its orphan record, and a drop without retention
            // (FORCE, or a branch database) to the purge record its statement added first.
            SchemaOp.DropTable => entry.GetPayload<SchemaDropTablePayload>() is var drop
                && (drop.Deferred || PurgesTableData(drop.TableName)),

            _ => false
        };

        if (!supported)
            throw new CamusDBException(
                CamusDBErrorCodes.StatementNotAllowedInTransaction,
                $"Schema change '{entry.Op}' cannot yet run inside an explicit transaction with transactional DDL on; " +
                "commit or roll back first, then run it");
    }
}
