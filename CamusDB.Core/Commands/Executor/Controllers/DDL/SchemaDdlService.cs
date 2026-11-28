
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Catalogs.Transactional;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Executes schema DDL against one database: create/drop/rename/relink table, add and drop columns
/// and indexes, constraints, and comments. Every statement here either runs locally or is handed to
/// <see cref="DdlForwardingCoordinator"/> first, so a follower never mutates schema itself.
///
/// <para><b>There are two execution shapes, and picking the wrong one is the classic bug.</b>
/// <see cref="ExecuteDdlInTransaction"/> is the single-transaction shape for metadata-only work. The
/// staged-coordinator shape drives an element through <c>Absent → DeleteOnly → WriteOnly → Public</c>
/// (<see cref="AddColumnAsync"/>, <see cref="ExecuteStagedAddIndexAsync"/>), or an index back down
/// <c>Public → WriteOnly → DeleteOnly → Absent</c> (<see cref="ExecuteIndexDdlAsync"/> for a drop), in a
/// cluster and on a standalone node alike. So the cluster is never in a state where one node writes an
/// element another node cannot read, or reads an index another node stopped maintaining, and a column or
/// an index is never readable before the existing rows are in it. A dropped index's entries are purged in
/// the background after the drop (<see cref="DropIndexStagedWithinGateAsync"/>).</para>
///
/// <para><b>Every path enters the database's <see cref="SchemaDdlGate"/> for the tables it names,
/// and every path releases it in a <c>finally</c> that also fires the deferred step-down.</b> The gate's
/// semaphore is what makes "resolve the target, then mutate it" atomic against a concurrent
/// drop/recreate of the same name — which is why it is taken in standalone mode too, not only in
/// cluster mode. Entering for a table also waits for a staged change in progress on that table: a
/// staged job holds its table and takes the semaphore only around each delta it proposes, so its
/// backfill or validation pass, the part that takes minutes on a loaded table, no longer holds every
/// other DDL of the database. A statement that hands a job to the coordinator says so with
/// <c>gateHeldByCaller: true</c> and finds the gate held again when the job returns. The step-down
/// must fire after the KV transaction has settled, never before, or leadership can move while a write
/// is still in flight.</para>
///
/// <para>Partial failures compensate rather than unwind: a backfill that spans batches has already
/// committed the earlier ones, so a staged index add that fails drops the index on every node, deletes
/// its coordinator job and purges the entries
/// (<see cref="SchemaChangeCoordinator.TakeBackIndexAsync(DatabaseDescriptor, SchemaChangeJob, IndexBuildInfo, long, Exception)"/>).</para>
/// </summary>
internal sealed partial class SchemaDdlService
{
    private readonly ExecutorContext context;

    /// <summary>Configuration for this engine; injected, never ambient. See <see cref="ApplyOptions"/>.</summary>
    private CamusDBOptions options;

    private readonly CatalogsManager catalogs;

    private readonly DdlForwardingCoordinator ddlForwarding;

    private readonly TableCreator tableCreator;

    /// <summary>Creates and drops the sequences an identity column owns.</summary>
    private readonly SequenceDdlService sequenceDdl;

    private readonly TableColumnAlterer tableColumnAlterer;

    private readonly TableIndexAlterer tableIndexAlterer;

    private readonly TableConstraintAlterer tableConstraintAlterer;

    private readonly CommentSetter commentSetter;

    private readonly TableDropper tableDropper;

    private readonly RowDeleter rowDeleter;

    private readonly QueryExecutor queryExecutor;

    private readonly SqlParserCache sqlParserCache;

    // DDL transaction commits must be bounded — LocateAndCommitTransaction with
    // CancellationToken.None can hang indefinitely if the schema partition Raft actor
    // is stalled. 10 s covers leader-election time in a healthy cluster while still
    // converting permanent stalls into a recoverable CamusDBException.
    private static readonly TimeSpan DdlCommitTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Test-only hook: invoked after each intermediate batch checkpoint is persisted
    /// (i.e., after batch N commits and before batch N+1 starts). Allows tests to inject
    /// a pause or forced leader change between batches without relying on timing.
    /// Set to null in production; cleared in test TearDown.
    /// </summary>
    internal Func<Task>? TestInterceptAfterBackfillCheckpoint;

    /// <summary>
    /// Test-only hook: invoked by <c>ADD COLUMN</c> after the check that refuses NOT NULL without a
    /// value on a table with rows, and before the column is added. A test inserts a row at that point
    /// to reach the fill-time refusal and the removal of the column. Null in production; a test clears
    /// it after use.
    /// </summary>
    internal Func<Task>? TestInterceptAfterAddColumnPrecheck;

    /// <summary>
    /// Test-only hook: invoked by <c>ADD COLUMN</c> after each staged step of the column commits
    /// (<c>DeleteOnly</c>, <c>WriteOnly</c>, <c>Public</c>), with the state reached. A test writes rows
    /// in a given state, or throws to stop the statement there. Null in production; a test clears it
    /// after use.
    /// </summary>
    internal Func<SchemaElementState, Task>? TestInterceptAfterAddColumnStep;

    /// <summary>
    /// Test-only hook: invoked after the backfill of a new index wrote its last batch and before the step
    /// to <c>Public</c>. A test writes rows at that point, which the statements themselves must index. Null
    /// in production; a test clears it after use.
    /// </summary>
    internal Func<Task>? TestInterceptAfterIndexBackfill;

    /// <summary>
    /// Test-only hook: invoked by <c>CREATE INDEX</c> after its early checks and before it
    /// enters the DDL gate. A test commits another schema change at that point, such as a DROP COLUMN
    /// of a column the index reads. Null in production; a test clears it after use.
    /// </summary>
    internal Func<Task>? TestInterceptBeforeAddIndexGate;

    /// <summary>
    /// Test-only hook: invoked by <c>DROP INDEX</c> outside a transaction after each step of the index
    /// commits (<c>WriteOnly</c>, <c>DeleteOnly</c>, <c>Absent</c>), with the state reached, before the next
    /// step. A test reads and writes the table in a given state, or throws to stop the statement there. Null
    /// in production; a test clears it after use.
    /// </summary>
    internal Func<SchemaElementState, Task>? TestInterceptAfterIndexDropStep;

    /// <summary>
    /// Test-only hook: invoked by <c>DROP INDEX</c> outside a transaction after its job is written and before
    /// its first step, with the index still <c>Public</c>. A test throws to stop the drop there. Null in
    /// production; a test clears it after use.
    /// </summary>
    internal Func<Task>? TestInterceptBeforeIndexDropFirstStep;

    /// <summary>
    /// Test-only hook: invoked before the job of a dropped index is replaced by its purge record, by a drop
    /// and by a <c>CREATE INDEX</c> that finishes an earlier drop of the same name. A test throws to fail that
    /// write. Null in production; a test clears it after use.
    /// </summary>
    internal Func<Task>? TestInterceptBeforeIndexDropRecord;

    /// <summary>
    /// Test-only hook: invoked by <c>DROP INDEX</c> outside a transaction after the caller opened the table
    /// and before the statement enters the DDL gate. A test drops or replaces the table at that point. Null
    /// in production; a test clears it after use.
    /// </summary>
    internal Func<Task>? TestInterceptBeforeIndexDropGate;

    internal SchemaDdlService(
        ExecutorContext context,
        CamusDBOptions options,
        CatalogsManager catalogs,
        DdlForwardingCoordinator ddlForwarding,
        TableCreator tableCreator,
        SequenceDdlService sequenceDdl,
        TableColumnAlterer tableColumnAlterer,
        TableIndexAlterer tableIndexAlterer,
        TableConstraintAlterer tableConstraintAlterer,
        CommentSetter commentSetter,
        TableDropper tableDropper,
        RowDeleter rowDeleter,
        QueryExecutor queryExecutor,
        SqlParserCache sqlParserCache
    )
    {
        this.context = context;
        this.options = options;
        this.catalogs = catalogs;
        this.ddlForwarding = ddlForwarding;
        this.tableCreator = tableCreator;
        this.sequenceDdl = sequenceDdl;
        this.tableColumnAlterer = tableColumnAlterer;
        this.tableIndexAlterer = tableIndexAlterer;
        this.tableConstraintAlterer = tableConstraintAlterer;
        this.commentSetter = commentSetter;
        this.tableDropper = tableDropper;
        this.rowDeleter = rowDeleter;
        this.queryExecutor = queryExecutor;
        this.sqlParserCache = sqlParserCache;
    }

    /// <summary>
    /// Swaps in a newly published configuration snapshot. Reference assignment is atomic and the
    /// record itself stays immutable; each statement pins the field once, so an in-flight statement
    /// keeps the snapshot it started with.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next) => options = next;

    /// <summary>
    /// Runs a DDL action inside a <see cref="KvTransaction"/> and commits it. Calls
    /// <paramref name="postCommitInvalidate"/> after a successful commit so the query-result cache
    /// evicts any entries whose schema deps reference the affected table. Schema meta keys use the
    /// <c>{db}/meta/…</c> keyspace and do not match the row/index bucket patterns tracked by
    /// <see cref="KvTransactionsManager"/>'s automatic key-based invalidation, so DDL paths must
    /// supply an explicit invalidation action here rather than relying on the DML commit hook.
    /// </summary>
    /// <remarks>
    /// <b>Intentional asymmetry vs DML invalidation:</b> DML commits drive
    /// <c>CachePublishGate.MarkWriteInFlight / CommitWrite</c> so the invalidation runs inside a
    /// critical section that also bumps the generation counter, atomically fencing concurrent
    /// TryPublishUnderGeneration calls. DDL invalidation (<paramref name="postCommitInvalidate"/>)
    /// runs after <c>CommitAsync</c> returns, outside that critical section — there is a narrow
    /// window in which a concurrent SELECT could publish a stale-schema entry.
    ///
    /// This asymmetry is acceptable: schema meta keys cannot participate in the key-based gate
    /// (they do not map to a table bucket). However, the window is closed at the read side:
    /// <see cref="QueryExecutor"/> performs an in-memory schema dep re-check on every hit
    /// (non-strict and strict alike). An entry that manages to publish in this narrow window
    /// will be evicted on the very first subsequent probe when its schema version is found stale,
    /// so it can never be served.
    /// </remarks>
    /// <param name="tableNames">
    /// The tables the statement names: the one it changes, and for a rename the new name too. The gate
    /// waits for a staged change in progress on any of them before the transaction starts, so a drop,
    /// a rename or a column change never runs under a backfill of the same table. Null for a change
    /// that names no table.
    /// </param>
    internal async Task<T> ExecuteDdlInTransaction<T>(
        DatabaseDescriptor database,
        Func<KvTransaction, Task<T>> action,
        Func<Task>? onAbort = null,
        Action? postCommitInvalidate = null,
        IReadOnlyList<string>? tableNames = null
    )
    {
        // Inside an explicit transaction with a private schema, the statement's writes go into that
        // transaction and its delta into its change set; the transaction's COMMIT or ROLLBACK decides
        // both. Nothing commits here, so there is nothing to compensate or to invalidate yet.
        if (TransactionSchemaScope.OverlayFor(database) is { } overlay)
            return await ExecuteDdlInOwnerTransactionAsync(database, overlay, action, tableNames).ConfigureAwait(false);

        // Acquired in BOTH modes. It used to be cluster-only, on the reasoning that a standalone node
        // has no replicated schema log to order proposals against — but the gate is also what makes
        // "resolve the target, then mutate it" atomic against a concurrent drop/recreate of the same
        // object. Skipping it standalone meant metadata-only DDL that *does* take the gate (COMMENT ON,
        // ALTER TABLE SET) serialized against nothing there, and could persist a blob for a table that
        // had just been dropped. One discipline in both modes; DDL is rare enough that the added
        // serialization costs nothing that matters.
        if (tableNames is not null)
            await database.DdlGate.EnterAsync(tableNames, context.Logger).ConfigureAwait(false);
        else
            await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            mutationLimitOverride: 0
        ).ConfigureAwait(false);
        try
        {
            T result = await action(tx).ConfigureAwait(false);
            using CancellationTokenSource cts = new(DdlCommitTimeout);
            await database.Transactions.CommitAsync(tx, cts.Token).ConfigureAwait(false);
            postCommitInvalidate?.Invoke();
            return result;
        }
        catch (Exception ex)
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);

            if (onAbort is not null)
            {
                try
                {
                    await onAbort().ConfigureAwait(false);
                }
                catch (Exception cleanupEx)
                {
                    context.Logger.LogWarning(
                        cleanupEx,
                        "Failed to run DDL abort compensation for database {DatabaseName} after {ErrorType}",
                        database.Name,
                        ex.GetType().Name
                    );
                }
            }

            throw;
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
            // Fire after CommitAsync (or RollbackIfNotCompletedAsync on error) so the
            // KV transaction is settled before schema-partition leadership changes.
            await FireDeferredStepDownIfRequestedAsync(database).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// <see cref="ExecuteDdlInTransaction"/> for a statement of a transaction with a private schema: runs
    /// <paramref name="action"/> in that transaction, under the DDL gate as every DDL statement runs, and
    /// commits nothing. A failure fails the statement, which marks the transaction rollback-only.
    /// </summary>
    private async Task<T> ExecuteDdlInOwnerTransactionAsync<T>(
        DatabaseDescriptor database,
        TransactionSchemaOverlay overlay,
        Func<KvTransaction, Task<T>> action,
        IReadOnlyList<string>? tableNames)
    {
        if (tableNames is not null)
            await database.DdlGate.EnterAsync(tableNames, context.Logger).ConfigureAwait(false);
        else
            await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);

        try
        {
            return await action(overlay.Owner).ConfigureAwait(false);
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
        }
    }

    public async Task<CreateTableResult> CreateTable(CreateTableTicket ticket)
    {
        context.Validator.Validate(ticket);

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        ForeignKeyPrivileges.RequireReferencePrivileges(database, ticket);

        bool? forwarded = await ddlForwarding.TryForwardCreateTableAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return new CreateTableResult(database, forwarded.Value);

        // Allocate the table id before the DDL transaction — only the proposer/leader allocates;
        // the id is carried in the replicated payload so every follower applies the same id.
        DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);
        string tableId = await registry.AllocateTableIdAsync().ConfigureAwait(false);

        bool created = await CreateTableWithSequencesAsync(database, ticket, tableId).ConfigureAwait(false);
        return new CreateTableResult(database, created);
    }

    /// <summary>
    /// Creates one table, including the sequences its identity columns own. Both entry points into
    /// <c>CREATE TABLE</c> — the ticket API and the SQL dispatcher — go through here, so neither
    /// can be the one that forgets to desugar an identity column.
    /// </summary>
    /// <param name="tableId">
    /// The id the proposer allocated. It is taken as an argument rather than allocated here because
    /// the caller has already paid for it, and because an identity column's sequence records it as
    /// its owner before the table exists.
    /// </param>
    internal async Task<bool> CreateTableWithSequencesAsync(
        DatabaseDescriptor database, CreateTableTicket ticket, string tableId)
    {
        // Answered before a single sequence is created. Without it, every re-run of
        // CREATE TABLE IF NOT EXISTS on a table that already exists minted one more sequence for
        // each of its identity columns and then returned "did nothing".
        if (ticket.IfNotExists && catalogs.TableExists(database, ticket.TableName))
            return false;

        // The compensation list is owned by this method and handed down, so a failure part-way
        // through resolution still reports what it had already created. Built here rather than
        // returned by the resolver for exactly that reason: a resolver that throws returns nothing,
        // and the sequences it had already made would never be cleaned up.
        List<string> createdSequenceNames = [];

        // In a cluster a foreign key is born WriteOnly and published only after the validation pass;
        // a standalone node has no second schema version and the new table holds no rows, so its
        // constraints are born Public.
        // Inside a transaction with a private schema, the constraints are born Public: no other
        // transaction can see the table, or write it, before the commit publishes it on every node.
        bool stagedForeignKeys = context.IsClusterMode && ticket.ForeignKeys.Length > 0
            && TransactionSchemaScope.OverlayFor(database) is null;
        SchemaElementState foreignKeyState = stagedForeignKeys ? SchemaElementState.WriteOnly : SchemaElementState.Public;
        bool created = false;

        try
        {
            // Sequence-backed columns are resolved before the table's own delta: a column stores
            // the sequence's immutable id, so the sequence has to exist first.
            await ResolveSequenceColumnsAsync(
                database, ticket, tableId, createdSequenceNames, CancellationToken.None).ConfigureAwait(false);

            // The rollout jobs are recorded before the table exists, so no crash can leave a WriteOnly
            // constraint without a job to finish it. A job whose table never appeared is removed by
            // the next resume, which treats a missing table id as stale.
            if (stagedForeignKeys)
                await PersistForeignKeyJobsAsync(database, ticket, tableId).ConfigureAwait(false);

            CreateTableResult result = await ExecuteDdlInTransaction(database, async tx =>
            {
                bool ok = await tableCreator.Create(
                    queryExecutor, context.TableOpener, tableIndexAlterer, database, ticket, tx, tableId, foreignKeyState).ConfigureAwait(false);
                return new CreateTableResult(database, ok);
            },
            tableNames: [ticket.TableName]).ConfigureAwait(false);

            created = result.Success;

            // A create that did nothing — the IF NOT EXISTS arm losing a race with another node —
            // leaves the sequences it minted with no owner. Cleaned up on that path too, not only
            // on the throwing one.
            if (!result.Success)
            {
                await DropSequencesAfterFailedCreateAsync(database, createdSequenceNames).ConfigureAwait(false);

                if (stagedForeignKeys)
                    await DeleteForeignKeyJobsAsync(database, ticket, tableId).ConfigureAwait(false);

                return false;
            }
        }
        catch
        {
            await DropSequencesAfterFailedCreateAsync(database, createdSequenceNames).ConfigureAwait(false);

            if (stagedForeignKeys)
                await DeleteForeignKeyJobsAsync(database, ticket, tableId).ConfigureAwait(false);

            throw;
        }

        // The table exists from here on. A failure while publishing leaves each constraint enforced in
        // WriteOnly with its job recorded, which a resumed leader finishes; nothing is rolled back.
        if (created && stagedForeignKeys)
            await PublishForeignKeysAsync(database, ticket, tableId).ConfigureAwait(false);

        return created;
    }

    /// <summary>
    /// Validates each foreign key of a table created in cluster mode and moves it from
    /// <c>WriteOnly</c> to <c>Public</c>, through the schema-change coordinator. The CREATE TABLE delta
    /// already passed its post-commit ack gate, so every live node enforces the constraints; the
    /// validation pass then sees any orphan that the window before the ack could make.
    ///
    /// <para>A constraint that fails validation is removed and the others are still published; the
    /// statement then fails with the first violation. The table stays: its rows were written by other
    /// sessions after it became visible, and the error says how to add the constraint again.</para>
    /// </summary>
    private async Task PublishForeignKeysAsync(DatabaseDescriptor database, CreateTableTicket ticket, string tableId)
    {
        CamusDBException? firstViolation = null;

        await database.DdlGate.EnterAsync(ticket.TableName, context.Logger).ConfigureAwait(false);
        try
        {
            SchemaChangeCoordinator coordinator = new(catalogs, context.Logger)
            {
                ForeignKeyValidationAsync = (db, tableName, constraintName) => ValidateForeignKeyRowsAsync(db, tableName, constraintName),
                DropIndexAsync = (db, tableName, indexName) => DropIndexWithinGateAsync(db, tableName, indexName)
            };

            foreach (ForeignKeyInfo foreignKey in ticket.ForeignKeys)
            {
                try
                {
                    await coordinator.RunJobAsync(
                        database,
                        new SchemaChangeJob(database.Name, ticket.TableName, tableId, foreignKey.Name, SchemaElementState.Public, SchemaElementKind.ForeignKey),
                        gateHeldByCaller: true
                    ).ConfigureAwait(false);
                }
                catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.ForeignKeyViolation)
                {
                    firstViolation ??= new CamusDBException(
                        CamusDBErrorCodes.ForeignKeyViolation,
                        $"{ex.Message}. Table '{ticket.TableName}' was created without this constraint; " +
                        "remove the rows that have no parent, then add it with ALTER TABLE ... ADD CONSTRAINT");
                }
            }
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
            await FireDeferredStepDownIfRequestedAsync(database).ConfigureAwait(false);
        }

        if (firstViolation is not null)
            throw firstViolation;
    }

    /// <summary>Runs the foreign-key validation pass for one constraint of <paramref name="tableName"/>.</summary>
    internal Task ValidateForeignKeyRowsAsync(DatabaseDescriptor database, string tableName, string constraintName) =>
        ForeignKeyValidationPass.ValidateAsync(database, context.TableOpener, tableName, constraintName);

    /// <summary>Reads every row of <paramref name="tableName"/> against one CHECK or NOT NULL constraint.</summary>
    internal Task ValidateRowConstraintAsync(DatabaseDescriptor database, string tableName, SchemaElementKind kind, string constraintName) =>
        RowConstraintValidationPass.ValidateAsync(database, context.TableOpener, tableName, kind, constraintName);

    private async Task PersistForeignKeyJobsAsync(DatabaseDescriptor database, CreateTableTicket ticket, string tableId)
    {
        foreach (ForeignKeyInfo foreignKey in ticket.ForeignKeys)
        {
            await catalogs.PersistCoordinatorJobAsync(database, new PersistedCoordinatorJob
            {
                TableName = ticket.TableName,
                TableId = tableId,
                ElementName = foreignKey.Name,
                TargetState = SchemaElementState.Public,
                ElementKind = SchemaElementKind.ForeignKey,
            }).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Best-effort removal of the jobs recorded for a CREATE TABLE that did not create the table. A job
    /// left behind is harmless: its table id never becomes live, and a resume deletes it.
    /// </summary>
    private async Task DeleteForeignKeyJobsAsync(DatabaseDescriptor database, CreateTableTicket ticket, string tableId)
    {
        foreach (ForeignKeyInfo foreignKey in ticket.ForeignKeys)
        {
            try
            {
                await catalogs.DeleteCoordinatorJobAsync(database, tableId, foreignKey.Name).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                context.Logger.LogWarning(
                    ex,
                    "Failed to delete the foreign-key rollout job {ConstraintName} of a table that was not created in database {DatabaseName}",
                    foreignKey.Name,
                    database.Name);
            }
        }
    }

    /// <summary>
    /// Turns every sequence-backed column declaration into a resolved sequence id: creates the
    /// owned sequence behind a <c>SERIAL</c> or <c>GENERATED AS IDENTITY</c> column, and resolves
    /// the name in an explicit <c>DEFAULT nextval('…')</c>.
    /// </summary>
    /// <remarks>
    /// <para><b>The sequences are created before the table, and that order is forced.</b> A column
    /// records the sequence's immutable id, so the id has to exist before the <c>CREATE TABLE</c>
    /// delta is built. The alternative — create the table, then the sequences, then alter the
    /// columns — is two more schema versions and a window in which the table exists with no
    /// default at all.</para>
    ///
    /// <para>The cost is that a <c>CREATE TABLE</c> which fails after this point has already
    /// created sequences. The caller drops them; see
    /// <see cref="DropSequencesAfterFailedCreateAsync"/> for what happens when that cleanup
    /// itself fails.</para>
    /// </remarks>
    /// <param name="created">
    /// The caller's compensation list, appended to as each sequence is created. Passed in rather
    /// than returned because this method can throw part-way through, and a return value would
    /// carry nothing when it does — leaving the sequences it had already made unreachable.
    /// </param>
    private async Task ResolveSequenceColumnsAsync(
        DatabaseDescriptor database,
        CreateTableTicket ticket,
        string tableId,
        List<string> created,
        CancellationToken cancellationToken)
    {
        for (int i = 0; i < ticket.Columns.Length; i++)
            ticket.Columns[i] = await ResolveSequenceColumnAsync(
                database, ticket.TableName, tableId, ticket.Columns[i], created, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Resolves one column declaration that draws from a sequence, for <c>CREATE TABLE</c> and for
    /// <c>ALTER TABLE … ADD COLUMN</c>. An identity column gets a new sequence owned by
    /// <paramref name="tableId"/>; <c>DEFAULT nextval('…')</c> is resolved to the existing sequence's
    /// id. Returns the column with <see cref="ColumnInfo.DefaultSequenceId"/> set, or the column
    /// unchanged when it draws from no sequence.
    /// </summary>
    /// <param name="created">
    /// The caller's compensation list. The name of a sequence this call creates is appended before
    /// the call returns, so the caller can drop it when a later step fails.
    /// </param>
    /// <param name="alsoFreeIn">
    /// A second schema in which the name of a new sequence must be free: the private schema of the explicit
    /// transaction that stages the column on a shared table. Null when there is none.
    /// </param>
    private async Task<ColumnInfo> ResolveSequenceColumnAsync(
        DatabaseDescriptor database,
        string tableName,
        string tableId,
        ColumnInfo column,
        List<string> created,
        CancellationToken cancellationToken,
        Schema? alsoFreeIn = null)
    {
        if (column.Identity is null && column.DefaultSequenceName is null)
            return column;

        string sequenceId;

        if (column.Identity is not null)
        {
            string sequenceName = BuildOwnedSequenceName(database.Schema, alsoFreeIn, tableName, column.Name);

            SequenceSchema owned = await sequenceDdl.CreateOwnedAsync(
                database,
                sequenceName,
                startValue: 1,
                increment: 1,
                minValue: 1,
                maxValue: null,
                // The same cache a plain CREATE SEQUENCE gets, for the same reason: a serial
                // column that skipped a thousand values on every restart while a hand-written
                // sequence did not would be an exception nobody could predict.
                cacheSize: 1,
                ownedByTableId: tableId,
                cancellationToken).ConfigureAwait(false);

            created.Add(sequenceName);
            sequenceId = owned.Id!;
        }
        else
        {
            sequenceId = RequireDefaultSequence(database.Schema, column).Id!;
        }

        return new ColumnInfo(
            column.Name,
            column.Type,
            column.NotNull,
            column.Default,
            column.MaxLength,
            column.ArrayElementType,
            column.DefaultFunction,
            column.NotNullConstraintName,
            column.Comment,
            column.Storage,
            defaultSequenceId: sequenceId,
            identityAlways: column.IdentityAlways,
            identity: column.Identity,
            defaultSequenceName: column.DefaultSequenceName,
            integerBits: column.IntegerBits);
    }

    /// <summary>
    /// The sequence of <paramref name="schema"/> that the default of <paramref name="column"/> draws from.
    /// Refuses a sequence that does not exist, and one that an identity column owns.
    /// </summary>
    private static SequenceSchema RequireDefaultSequence(Schema schema, ColumnInfo column)
    {
        if (!schema.Sequences.TryGetValue(column.DefaultSequenceName!, out SequenceSchema? existing))
            throw new CamusDBException(
                CamusDBErrorCodes.SequenceDoesntExist,
                $"Column '{column.Name}' defaults from sequence '{column.DefaultSequenceName}', which does not exist");

        // An owned sequence belongs to its identity column and goes when that column goes,
        // with no dependency check on the way out. A default in a second column would
        // therefore be left pointing at a counter that is gone the moment the owner is
        // dropped — so the reference is refused here rather than allowed to dangle later.
        if (existing.OwnedByTableId is { Length: > 0 })
            throw new CamusDBException(
                CamusDBErrorCodes.SequenceInUse,
                $"Column '{column.Name}' cannot default from sequence '{existing.Name}': that sequence is " +
                $"owned by an identity column of another relation, and is dropped with it. " +
                "Create a free-standing sequence with CREATE SEQUENCE and default from that instead.");

        return existing;
    }

    /// <summary>
    /// The name an identity column's own sequence gets: <c>{table}_{column}_seq</c>, with a numeric
    /// suffix when that name is taken in <paramref name="schema"/> or in <paramref name="alsoFreeIn"/>.
    /// </summary>
    /// <remarks>
    /// The availability check and the create that follows are not one atomic step, so two
    /// concurrent <c>CREATE TABLE</c> statements racing for the same generated name can both pass
    /// here. The loser does not overwrite the winner: the create is refused under the schema lock
    /// with "relation already exists", which fails that statement rather than corrupting the other.
    /// </remarks>
    private static string BuildOwnedSequenceName(Schema schema, Schema? alsoFreeIn, string tableName, string columnName)
    {
        string baseName = $"{tableName}_{columnName}_seq";

        bool IsTaken(string name) => IsRelationNameTaken(schema, name) || (alsoFreeIn is not null && IsRelationNameTaken(alsoFreeIn, name));

        if (!IsTaken(baseName))
            return baseName;

        for (int suffix = 1; suffix < 1000; suffix++)
        {
            string candidate = $"{baseName}{suffix}";

            if (!IsTaken(candidate))
                return candidate;
        }

        throw new CamusDBException(
            CamusDBErrorCodes.SequenceAlreadyExists,
            $"Cannot name the sequence for '{tableName}.{columnName}': '{baseName}' and every suffixed " +
            "variant of it are taken. Create the sequence yourself and use DEFAULT nextval('...').");
    }

    private static bool IsRelationNameTaken(Schema schema, string name)
        => schema.Tables.ContainsKey(name)
           || schema.Views.ContainsKey(name)
           || schema.Sequences.ContainsKey(name);

    /// <summary>
    /// Removes the sequences a failed <c>CREATE TABLE</c> had already created, so the statement
    /// leaves nothing behind.
    /// </summary>
    /// <remarks>
    /// Best effort, and deliberately so: the create has already failed and is about to be
    /// rethrown, so a cleanup that throws in turn would replace the real error with an incidental
    /// one. A sequence left behind by a failed cleanup is visible in <c>SHOW SEQUENCES</c> and can
    /// be dropped by hand, which is a far better outcome than losing the cause.
    /// </remarks>
    private async Task DropSequencesAfterFailedCreateAsync(DatabaseDescriptor database, List<string> sequenceNames)
    {
        foreach (string sequenceName in sequenceNames)
        {
            try
            {
                await sequenceDdl.DropByNameAsync(database, sequenceName, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception cleanupEx)
            {
                context.Logger.LogWarning(
                    cleanupEx,
                    "A CREATE TABLE or ADD COLUMN failed and the sequence '{Sequence}' it had created for database " +
                    "{DatabaseName} could not be removed; drop it by hand",
                    sequenceName,
                    database.Name);
            }
        }
    }

    public async Task<bool> AlterTable(AlterTableTicket ticket)
    {
        context.Validator.Validate(ticket);

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        // Checked before the statement can be forwarded, so the caller is refused here rather than
        // learning about it from another node. The node that ultimately applies the change runs this
        // same path and re-checks against its own schema.
        RequireNoViewDependsOnAlteredColumn(database, ticket);

        bool? forwarded = await ddlForwarding.TryForwardAlterTableAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return forwarded.Value;

        TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        return await AlterColumnWithSequencesAsync(database, table, ticket).ConfigureAwait(false);
    }

    /// <summary>
    /// Adds or drops one column and, when a dropped column owned a sequence, removes that sequence
    /// too. Both entry points into <c>ALTER TABLE … ADD/DROP COLUMN</c> go through here.
    /// </summary>
    /// <remarks>
    /// <para>Only an <b>owned</b> sequence goes — one created for this column by <c>SERIAL</c> or
    /// <c>GENERATED AS IDENTITY</c>. A column that merely defaults from a free-standing sequence
    /// leaves it alone: other columns may draw from it, and a shared counter is not this
    /// statement's to remove.</para>
    ///
    /// <para>The column goes first, the counter after, for the same reason a dropped table's does:
    /// removing the counter while the column can still be written would fail an insert in that
    /// window, while the other order leaves at worst an unreachable counter.</para>
    /// </remarks>
    internal async Task<bool> AlterColumnWithSequencesAsync(
        DatabaseDescriptor database, TableDescriptor table, AlterTableTicket ticket)
    {
        if (ticket.Operation == AlterTableOperation.AddColumn)
            return await AddColumnAsync(database, table, ticket).ConfigureAwait(false);

        // Resolved before the drop: afterwards the column is gone and nothing links the sequence to
        // it any more.
        List<string> orphanedSequences = ticket.Operation == AlterTableOperation.DropColumn
            ? CollectSequencesOwnedByColumn(database, table, ticket.Column.Name)
            : [];

        bool altered = await ExecuteDdlInTransaction(database,
            tx => tableColumnAlterer.Alter(queryExecutor, database, table, ticket, tx),
            postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, table.Id),
            tableNames: [ticket.TableName]
        ).ConfigureAwait(false);

        // Inside a transaction with a private schema the sequence's catalog record leaves with the change
        // set, and its counter only after the publish (see SequenceDdlService.DropByNameAsync).
        if (altered)
            await DropOwnedSequencesAsync(database, orphanedSequences).ConfigureAwait(false);

        return altered;
    }

    /// <summary>
    /// <c>ALTER TABLE … ADD COLUMN</c>, on a standalone node and in a cluster alike. The column goes
    /// through <c>Absent → DeleteOnly → WriteOnly → [fill] → Public</c> under a durable coordinator job,
    /// so it is readable only after every existing row holds its value (see <see cref="TableColumnAdder"/>).
    /// </summary>
    /// <remarks>
    /// <para><b>Why a standalone node stages the column too.</b> Published in one step, the column is
    /// readable before its rows are filled, and a function, sequence or NOT NULL column shows NULL until
    /// the fill commits. A crash in that window leaves the column published and never filled: the schema
    /// change is durable on its own, and nothing records that the fill is owed. The coordinator job is
    /// that record, and a restart finishes or removes the column (<c>DatabaseOpener</c>).</para>
    ///
    /// <para><b>Checks before the gate are fast refusals only.</b> The duplicate name, the constraint
    /// name and the column limit are checked again under <see cref="DatabaseDescriptor.SchemaDdlSemaphore"/>,
    /// on a table opened under it, because two statements can both pass the early checks. A NOT NULL
    /// column with no value to fill is refused on a table that has a row, as PostgreSQL does. That check
    /// reads without a lock: a row that arrives after it is caught by the fill, and the column is then
    /// removed again.</para>
    ///
    /// <para><b>An identity column's sequence is created under the gate, before the job</b>, for the
    /// reason <see cref="ResolveSequenceColumnsAsync"/> gives. A failure before the column exists drops
    /// it here. A failure of the fill removes the column through the job, which drops the sequence after
    /// the column is gone (<see cref="ReleaseOwnedSequenceAsync"/>).</para>
    /// </remarks>
    private async Task<bool> AddColumnAsync(DatabaseDescriptor database, TableDescriptor table, AlterTableTicket ticket)
    {
        ColumnInfo requested = ticket.Column;

        RequireColumnCanBeAdded(table, requested, options);

        if (TransactionSchemaScope.OverlayFor(database) is { } overlay)
            return await AddColumnInOwnerTransactionAsync(database, overlay, table, ticket).ConfigureAwait(false);

        await AddColumnStagedAsync(database, table, ticket, owner: null).ConfigureAwait(false);
        return true;
    }

    /// <summary>What <see cref="AddColumnStagedAsync"/> staged: the column as resolved, its table, and the deltas it took.</summary>
    private readonly record struct StagedColumn(ColumnInfo Column, string TableId, string TableName, int StepsTaken);

    /// <summary>
    /// The staged body of <see cref="AddColumnAsync"/>: the NOT NULL check, the identity sequence, and the
    /// coordinator job that adds, fills and publishes the column. With <paramref name="owner"/> the job
    /// belongs to that explicit transaction and stops after the fill with the column <c>WriteOnly</c>; the
    /// caller then adds the step to <c>Public</c> to the transaction's change set. The name of a new
    /// sequence is then free in the private schema of the transaction too, because the publish adds both to
    /// one schema.
    /// </summary>
    private async Task<StagedColumn> AddColumnStagedAsync(DatabaseDescriptor database, TableDescriptor table, AlterTableTicket ticket, TransactionSchemaOverlay? owner)
    {
        ColumnInfo requested = ticket.Column;

        if (requested.NotNull && !TableColumnAdder.HasFillValue(requested)
            && await TableHasAnyRowAsync(database, table).ConfigureAwait(false))
            throw TableColumnAdder.ContainsNullValues(table.Name, requested.Name);

        if (TestInterceptAfterAddColumnPrecheck is { } intercept)
            await intercept().ConfigureAwait(false);

        List<string> createdSequences = [];
        ColumnInfo? resolved = null;

        await database.DdlGate.EnterAsync(ticket.TableName, context.Logger).ConfigureAwait(false);
        try
        {
            // Opened under the gate: another ADD COLUMN may have committed since the caller opened it.
            TableDescriptor gated = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
            RequireColumnCanBeAdded(gated, requested, options);

            resolved = await ResolveSequenceColumnAsync(
                database, gated.Name, gated.Id, requested, createdSequences, CancellationToken.None, alsoFreeIn: owner?.Schema).ConfigureAwait(false);

            SchemaChangeCoordinator coordinator = NewColumnCoordinator();

            // The gate is released while the rows are filled; see SchemaChangeCoordinator.RunJobAsync.
            await coordinator.RunJobAsync(
                database,
                new SchemaChangeJob(database.Name, gated.Name, gated.Id, resolved.Name, SchemaElementState.Public, SchemaElementKind.Column, owner?.ChangeSetId),
                columnDefinition: resolved,
                gateHeldByCaller: true
            ).ConfigureAwait(false);

            database.Cache?.InvalidateByTableId(database.Id, gated.Id);

            // Each sequence an identity column created took one schema version, as each job step did.
            return new StagedColumn(resolved, gated.Id, gated.Name, coordinator.StepsProposed + createdSequences.Count);
        }
        catch
        {
            // A sequence that a removed column owned is already gone (the coordinator released it); one
            // that a column still draws from stays.
            if (createdSequences.Count > 0
                && !AnyColumnDrawsFrom(database, ticket.TableName, resolved?.DefaultSequenceId))
                await DropSequencesAfterFailedCreateAsync(
                    database, createdSequences.FindAll(name => database.Schema.Sequences.ContainsKey(name))).ConfigureAwait(false);

            throw;
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
            await FireDeferredStepDownIfRequestedAsync(database).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// <c>ADD COLUMN</c> inside a transaction with a private schema: one delta that adds the column as
    /// Public, with no staged steps.
    /// <list type="bullet">
    /// <item>A nullable column with no default runs on any table: an older row reads NULL for it, which is
    /// its value.</item>
    /// <item>On a table the transaction created, any column runs: the rows are the transaction's own, and
    /// it fills them itself, right after the delta (<see cref="TableColumnAdder.FillOwnRowsAsync"/>). A NOT
    /// NULL column with no value for a row fails the statement.</item>
    /// <item>On any other table, a column that needs a fill runs the staged add of the shared table, owned
    /// by the transaction (<see cref="StageColumnOnSharedTableAsync"/>).</item>
    /// </list>
    /// </summary>
    private async Task<bool> AddColumnInOwnerTransactionAsync(
        DatabaseDescriptor database, TransactionSchemaOverlay overlay, TableDescriptor table, AlterTableTicket ticket)
    {
        ColumnInfo requested = ticket.Column;
        bool needsFill = requested.NotNull || TableColumnAdder.HasFillValue(requested);

        if (needsFill && !overlay.CreatedTable(table.Id))
            return await StageColumnOnSharedTableAsync(database, overlay, table, ticket).ConfigureAwait(false);

        List<string> createdSequences = [];
        string tableName;
        ColumnInfo resolved;

        await database.DdlGate.EnterAsync(ticket.TableName, context.Logger).ConfigureAwait(false);
        try
        {
            TableDescriptor gated = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
            RequireColumnCanBeAdded(gated, requested, options);

            resolved = await ResolveSequenceColumnAsync(
                database, gated.Name, gated.Id, requested, createdSequences, CancellationToken.None).ConfigureAwait(false);

            await catalogs.ReplicateAddColumnInStateAsync(database, gated.Name, resolved, SchemaElementState.Public).ConfigureAwait(false);

            tableName = gated.Name;
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
        }

        if (!needsFill)
            return true;

        // Opened again after the delta: the private schema now has the column, so the rows decode with it
        // (NULL in every row written before it) and are written back with its value.
        KvTransaction tx = overlay.Owner;
        TableDescriptor added = await context.TableOpener.Open(database, tableName).ConfigureAwait(false);

        QueryTicket rows = new(
            txnState: tx,
            databaseName: database.Name,
            tableName: added.Name,
            index: null,
            projection: null,
            filters: null,
            where: null,
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null
        );

        await tableColumnAlterer.FillOwnRowsAsync(
            database, added, resolved.Name, tx, queryExecutor.Query(database, added, rows)).ConfigureAwait(false);

        return true;
    }

    /// <summary>
    /// <c>ADD COLUMN</c> with a fill inside a transaction with a private schema, on a table the transaction
    /// did not create: the staged add of <see cref="AddColumnAsync"/> runs on the shared table, owned by the
    /// transaction, and stops with the column <c>WriteOnly</c> and every row filled. Other transactions write
    /// the column from then on but cannot read it. The step to <c>Public</c> joins the change set; see
    /// <see cref="RunOwnedStagedChangeAsync"/>. An identity column's sequence is created in the shared
    /// schema with the column, and the removal of the column releases it.
    /// </summary>
    private Task<bool> StageColumnOnSharedTableAsync(
        DatabaseDescriptor database, TransactionSchemaOverlay overlay, TableDescriptor table, AlterTableTicket ticket)
    {
        const string statement = "ALTER TABLE ... ADD COLUMN with a default value, an identity or NOT NULL";

        // The steps resolve the sequence and the function of the default in the shared schema, so each must
        // be the one that the private schema has under that name.
        if (ticket.Column is { Identity: null, DefaultSequenceName: not null })
            TransactionalDdlRules.RequireSharedSequence(overlay, RequireDefaultSequence(database.Schema, ticket.Column), table, statement);

        if (ticket.Column.DefaultFunction is { } function)
            TransactionalDdlRules.RequireSharedFunction(overlay, function, table, statement);

        return RunOwnedStagedChangeAsync(database, overlay, table, statement, async () =>
        {
            TableDescriptor shared = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

            StagedColumn staged = await AddColumnStagedAsync(database, shared, ticket, overlay).ConfigureAwait(false);

            return new OwnedStagedElement(staged.TableId, staged.TableName, staged.StepsTaken, [staged.Column.Name], [(staged.Column.Name, SchemaElementKind.Column)]);
        });
    }

    /// <summary>
    /// Refuses a column that <paramref name="table"/> cannot take: a name that is taken in any state, a
    /// NOT NULL constraint name that is taken, or one column more than
    /// <see cref="CamusDBOptions.MaxColumnsPerTable"/>. Called before the gate as a fast refusal, and
    /// under it as the check that counts.
    /// </summary>
    private static void RequireColumnCanBeAdded(TableDescriptor table, ColumnInfo column, CamusDBOptions options)
    {
        if (FindColumn(table.Schema, column.Name) is not null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Duplicate column '{column.Name}'");

        if (column.NotNullConstraintName is { } constraintName)
            ConstraintNameRules.RequireUnused(table.Schema, constraintName);

        int maxColumns = options.MaxColumnsPerTable;
        if (maxColumns > 0 && (table.Schema.Columns?.Count ?? 0) + 1 > maxColumns)
            throw new CamusDBException(
                CamusDBErrorCodes.SchemaLimitExceeded,
                $"Table '{table.Name}' would exceed the maximum of {maxColumns} columns per table");
    }

    /// <summary>
    /// The coordinator an ADD COLUMN runs under, with the fill and the release of an owned sequence
    /// wired. The resume coordinator in <c>DatabaseOpener</c> wires the same two delegates.
    /// </summary>
    private SchemaChangeCoordinator NewColumnCoordinator() => new(catalogs, context.Logger)
    {
        BackfillAsync = BackfillColumnDefaultsAsync,
        ReleaseOwnedSequenceAsync = ReleaseOwnedSequenceAsync,
        OnStepCompleted = TestInterceptAfterAddColumnStep is { } intercept
            ? (state, _) => intercept(state)
            : null,
    };

    private static TableColumnSchema? FindColumn(TableSchema schema, string columnName) =>
        schema.Columns?.Find(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));

    /// <summary>True when a column of <paramref name="tableName"/> draws its default from the sequence.</summary>
    private static bool AnyColumnDrawsFrom(DatabaseDescriptor database, string tableName, string? sequenceId) =>
        sequenceId is not null
        && database.Schema.Tables.TryGetValue(tableName, out TableSchema? schema)
        && schema.Columns?.Exists(c => string.Equals(c.DefaultSequenceId, sequenceId, StringComparison.Ordinal)) == true;

    /// <summary>
    /// True when the table holds at least one row. Reads without a lock, so the answer can be stale by
    /// the time the caller acts on it; a caller uses it only to refuse early.
    /// </summary>
    private static async Task<bool> TableHasAnyRowAsync(DatabaseDescriptor database, TableDescriptor table)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly
        ).ConfigureAwait(false);
        try
        {
            await foreach ((ObjectIdValue _, ReadOnlyMemory<byte> _) in table.Store.ScanRows(tx, afterRowId: null).ConfigureAwait(false))
                return true;

            return false;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The sequences owned by <paramref name="table"/> that only the named column draws from.
    /// </summary>
    private static List<string> CollectSequencesOwnedByColumn(
        DatabaseDescriptor database, TableDescriptor table, string columnName)
    {
        List<string> owned = [];

        if (table.Schema.Columns is null)
            return owned;

        foreach (TableColumnSchema column in table.Schema.Columns)
        {
            if (!string.Equals(column.Name, columnName, StringComparison.OrdinalIgnoreCase))
                continue;

            if (column.DefaultSequenceId is not { Length: > 0 } sequenceId)
                continue;

            SequenceSchema? sequence = database.Schema.FindSequenceById(sequenceId);

            // Owned by this relation, or it is a shared sequence the column merely defaults from.
            if (sequence?.Name is { Length: > 0 } name
                && string.Equals(sequence.OwnedByTableId, table.Id, StringComparison.Ordinal))
                owned.Add(name);
        }

        return owned;
    }

    /// <summary>
    /// Refuses a <c>DROP COLUMN</c> or <c>RENAME COLUMN</c> that a view reads. Adding a column
    /// breaks nothing, so it is not checked.
    /// </summary>
    /// <remarks>
    /// Resolves the column to its immutable id first: views record what they read by id, and a name
    /// is exactly the thing these two statements are about to change.
    /// </remarks>
    internal static void RequireNoViewDependsOnAlteredColumn(DatabaseDescriptor database, AlterTableTicket ticket)
    {
        if (ticket.Operation is not (AlterTableOperation.DropColumn or AlterTableOperation.RenameColumn))
            return;

        if (!database.Schema.Tables.TryGetValue(ticket.TableName, out TableSchema? tableSchema))
            return;

        string columnName = ticket.Column.Name;

        TableColumnSchema? column = tableSchema.Columns?
            .FirstOrDefault(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));

        if (column?.Id is not { Length: > 0 } columnId)
            return;

        ViewDependencyMaintainer.RequireNoDependentViewsOnColumn(
            database.Schema, ticket.TableName, columnName, columnId,
            renaming: ticket.Operation == AlterTableOperation.RenameColumn);
    }

    /// <summary>
    /// Fills the newly added <paramref name="column"/> (in <c>WriteOnly</c>) in every row that does not
    /// hold a value for it, in committed batches. Used by the command-path coordinator and by the resume
    /// coordinator, so the fill is part of the resumable sequence. Only the column's name is used: the
    /// definition comes from the live schema, which a resumed job's rebuilt <see cref="ColumnInfo"/> may
    /// describe less completely.
    /// </summary>
    internal async Task BackfillColumnDefaultsAsync(DatabaseDescriptor database, string tableName, ColumnInfo column, CancellationToken cancellationToken)
    {
        TableDescriptor table = await context.TableOpener.Open(database, tableName).ConfigureAwait(false);

        await tableColumnAlterer.FillAddedColumnAsync(database, table, column.Name, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Drops the sequence <paramref name="sequenceId"/> after the identity column that owned it was
    /// removed by a failed ADD COLUMN. Does nothing when the sequence is gone, is not owned by
    /// <paramref name="tableId"/>, or is still the default of a column: a shared sequence, or one a
    /// column still points at, is not this removal's to drop.
    /// </summary>
    internal async Task ReleaseOwnedSequenceAsync(DatabaseDescriptor database, string tableId, string sequenceId)
    {
        SequenceSchema? sequence = database.Schema.FindSequenceById(sequenceId);

        if (sequence?.Name is not { Length: > 0 } name
            || !string.Equals(sequence.OwnedByTableId, tableId, StringComparison.Ordinal))
            return;

        foreach (TableSchema schema in database.Schema.Tables.Values)
        {
            if (schema.Columns?.Exists(c => string.Equals(c.DefaultSequenceId, sequenceId, StringComparison.Ordinal)) == true)
                return;
        }

        await sequenceDdl.DropByNameAsync(database, name, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Scans every existing row in <paramref name="tableName"/> and writes its index entries with the
    /// idempotent write of a backfill, in transactions of at most <see cref="IndexBackfiller.BatchRows"/>
    /// rows; after each full batch commits, invokes <paramref name="onCheckpoint"/> with the last row id so
    /// the coordinator can persist a resume offset. Called by <see cref="SchemaChangeCoordinator"/>
    /// just before the index transitions from <c>WriteOnly</c> to <c>Public</c>.
    ///
    /// <para>The batches run at <c>Background</c> admission priority on a standalone node: one node serves
    /// the build and the user traffic, and each batch re-enters the admission gate, so a saturated node
    /// serves the traffic first. In a cluster they run at the engine default.</para>
    /// </summary>
    internal async Task BackfillIndexEntriesAsync(
        DatabaseDescriptor database,
        string tableName,
        IndexBuildInfo indexInfo,
        string? startOffset,
        Func<string, Task>? onCheckpoint = null,
        CancellationToken cancellationToken = default
    )
    {
        CamusDBOptions currentOptions = options;

        TableDescriptor table = await context.TableOpener.Open(database, tableName).ConfigureAwait(false);

        ObjectIdValue? afterRowId = string.IsNullOrWhiteSpace(startOffset)
            ? null
            : ObjectId.ToValue(startOffset!);

        // The expressions are bound against the table as it is now: a rename since the job started
        // changes the names the expression reads, never the columns.
        string[] keyColumns = indexInfo.ColumnNames;
        BoundIndexExpression?[]? expressions = null;
        if (indexInfo.KeyExpressions is { Length: > 0 })
        {
            Dictionary<string, string> nameById = (table.Schema.Columns ?? []).ToDictionary(c => c.Id, c => c.Name, StringComparer.Ordinal);
            (keyColumns, expressions) = IndexExpressionBinder.BindKey(
                indexInfo.ColumnIds,
                indexInfo.KeyExpressions,
                IndexExpressionBinder.ColumnsOf(table.Schema.Columns),
                table.Name,
                id => nameById.GetValueOrDefault(id) ?? id);
        }

        IndexBackfiller.Target target = new(
            indexInfo.IndexId,
            indexInfo.IndexName,
            Unique: indexInfo.IndexType == IndexType.Unique,
            keyColumns,
            indexInfo.IncludeColumnNames is { Length: > 0 } includeColumnNames ? includeColumnNames : null,
            Inverted: indexInfo.IndexType == IndexType.Inverted,
            Expressions: expressions);

        // After each full batch: persist the cursor (the coordinator's callback), then run the test
        // seam, so a test can pause or force a leader change between two batches.
        async Task Checkpoint(string offset)
        {
            if (onCheckpoint is not null)
                await onCheckpoint(offset).ConfigureAwait(false);

            if (TestInterceptAfterBackfillCheckpoint is not null)
                await TestInterceptAfterBackfillCheckpoint().ConfigureAwait(false);
        }

        int indexedRows = await IndexBackfiller.BackfillAsync(
            database, table, target, afterRowId,
            priority: context.IsClusterMode ? null : TransactionPriority.Background,
            Checkpoint,
            context.Logger,
            currentOptions,
            cancellationToken).ConfigureAwait(false);

        Log.LogIndexBackfillComplete(context.Logger, indexedRows, indexInfo.IndexName);

        if (TestInterceptAfterIndexBackfill is { } intercept)
            await intercept().ConfigureAwait(false);
    }

    /// <summary>
    /// Drives the add-index sequence through the coordinator-owned staged path
    /// (<c>Absent → DeleteOnly → WriteOnly → [backfill] → Public</c>), on a standalone node and in a
    /// cluster alike. Every statement that adds an index to a table that other transactions see comes here.
    /// </summary>
    /// <remarks>
    /// <b>Checks before the gate are fast refusals only.</b> Another statement can commit between the
    /// caller's open of the table and this statement's entry into the DDL gate. A DROP COLUMN in that
    /// window would leave a definition built before it with the id of a column the table no longer has,
    /// and an expression index staged with such an id makes every fresh open of the table fail. So the
    /// definition that is staged is built again, with every check, on the table opened under the gate.
    ///
    /// <para><b>Why a standalone node stages the index too.</b> The backfill visits every row and waits for
    /// every open transaction that holds one, and the coordinator runs it with the gate released and the
    /// table held. A build in memory under the gate kept every DDL of the database waiting for that long.
    /// It cannot release the gate either, because the index it adds is not durable: another statement that
    /// persists the table would store an unfinished index with no job to finish it.</para>
    /// </remarks>
    internal async Task<bool> ExecuteStagedAddIndexAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterIndexTicket ticket
    )
    {
        CamusDBOptions currentOptions = options;

        if (BuildStagedIndexInfo(table, ticket, currentOptions) is null)
            return false;

        if (TestInterceptBeforeAddIndexGate is { } intercept)
            await intercept().ConfigureAwait(false);

        await database.DdlGate.EnterAsync(ticket.TableName, context.Logger).ConfigureAwait(false);
        try
        {
            TableDescriptor gated = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

            // The same name on another table: the table was dropped and created again after the
            // caller opened it.
            if (!string.Equals(gated.Id, table.Id, StringComparison.Ordinal))
                throw new CamusDBException(
                    CamusDBErrorCodes.ConcurrentSchemaChange,
                    $"Table '{ticket.TableName}' was replaced by another statement while index '{ticket.IndexName}' was being created; nothing was created");

            if (BuildStagedIndexInfo(gated, ticket, currentOptions) is not { } indexInfo)
                return false;

            await StageIndexWithinGateAsync(database, gated.Id, ticket.TableName, indexInfo).ConfigureAwait(false);

            database.Cache?.InvalidateByTableId(database.Id, gated.Id);

            // The step to Public evicted the table's descriptor. Opened again here, so a reader of the
            // descriptor cache, such as the statistics and the planners, finds it with the new index.
            await context.TableOpener.Open(database, gated.Name).ConfigureAwait(false);

            return true;
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
            await FireDeferredStepDownIfRequestedAsync(database).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Checks a <c>CREATE INDEX</c> against <paramref name="table"/> and builds the definition
    /// to stage: the column ids, the INCLUDE ids and the stored form of each expression. Null when the
    /// index exists and the statement says <c>IF NOT EXISTS</c>. The ids are only as current as
    /// <paramref name="table"/>, so the definition that is staged must come from a table opened under
    /// the DDL gate.
    /// </summary>
    private static IndexBuildInfo? BuildStagedIndexInfo(TableDescriptor table, AlterIndexTicket ticket, CamusDBOptions currentOptions)
    {
        if (ticket.IfNotExists && table.AllIndexes.ContainsKey(ticket.IndexName))
            return null;

        TableIndexAdder.Validate(table, ticket, currentOptions);

        IndexType indexType = ticket.Operation switch
        {
            AlterIndexOperation.AddUniqueIndex or AlterIndexOperation.AddPrimaryKey => IndexType.Unique,
            AlterIndexOperation.AddInvertedIndex => IndexType.Inverted,
            _ => IndexType.Multi,
        };

        // Column positions keep their own checks (GetColumnIdsForIndex); expression positions are
        // resolved by the binder, which gives them the marker id and the hidden key name.
        ResolvedIndexKey key = TableIndexAdder.ResolveKey(table, ticket);

        string indexId = ObjectIdGenerator.Generate().ToString();
        string[] columnIds = key.Expressions is null ? GetColumnIdsForIndex(table, ticket.Columns) : key.ColumnIds;
        string[] columnNames = key.Expressions is null ? ticket.Columns.Select(c => c.Name).ToArray() : key.ColumnNames;
        if (key.Expressions is not null)
            GetColumnIdsForIndex(table, [.. ticket.Columns.Where(c => !c.IsExpression)]);
        string[]? includeColumnIds = ticket.IncludeColumns.Length > 0
            ? GetColumnIdsForIndex(table, ticket.IncludeColumns.Select(n => new ColumnIndexInfo(n, OrderType.Ascending)).ToArray())
            : null;
        string[]? includeColumnNames = ticket.IncludeColumns.Length > 0 ? ticket.IncludeColumns : null;

        return new(indexId, ticket.IndexName, columnIds, columnNames, indexType, IndexColumnOrder.Extract(ticket.Columns), IncludeColumnIds: includeColumnIds, IncludeColumnNames: includeColumnNames, KeyExpressions: key.KeyExpressions);
    }

    /// <summary>
    /// Drives one new index through <c>Absent → DeleteOnly → WriteOnly → [backfill] → Public</c> with the
    /// schema-change coordinator, and removes it again if the sequence fails before Public. The caller
    /// entered the DDL gate for the table; the coordinator releases the gate while the backfill runs and
    /// holds it again when it returns (see <see cref="SchemaChangeCoordinator.RunJobAsync"/>), so the
    /// compensation below proposes under it. The add-index path and the foreign-key add path both call it.
    /// </summary>
    /// <remarks>
    /// With <paramref name="ownerChangeSetId"/> the job belongs to that explicit transaction: it stops after
    /// the backfill with the index still <c>WriteOnly</c>, and the transaction's change set holds the step
    /// to <c>Public</c>. Returns the number of schema deltas the job proposed.
    /// </remarks>
    private async Task<int> StageIndexWithinGateAsync(DatabaseDescriptor database, string tableId, string tableName, IndexBuildInfo indexInfo, string? ownerChangeSetId = null)
    {
        SchemaChangeCoordinator coordinator = NewStagedIndexCoordinator();

        SchemaChangeJob job = new(database.Name, tableName, tableId, indexInfo.IndexName, SchemaElementState.Public, SchemaElementKind.Index, ownerChangeSetId);

        try
        {
            await coordinator.RunJobAsync(database, job, indexBuildInfo: indexInfo, gateHeldByCaller: true).ConfigureAwait(false);

            return coordinator.StepsProposed;
        }
        catch (SchemaChangeHandedOverException)
        {
            // The job was handed over to the next schema leader and answered from its outcome: the
            // index is that leader's, built or dropped there, and nothing here is this node's to undo.
            throw;
        }
        catch (Exception ex)
        {
            // A run that failed before it wrote its own job proposed nothing, and the job key may still
            // hold another job, such as the drop of an earlier index of this name that the run was
            // settling. A take-back would write over that job.
            if (!coordinator.JobRecorded)
                throw;

            // The index can be in the schema on every node, short of Public. An owned job keeps its
            // record: the owner's rollback reads it to remove the entries the backfill wrote.
            if (ownerChangeSetId is null)
                await coordinator.TakeBackIndexAsync(database, job, indexInfo, coordinator.LeaderTerm, ex).ConfigureAwait(false);
            else
                await DropFailedOwnedIndexAsync(database, tableName, indexInfo.IndexName).ConfigureAwait(false);

            throw;
        }
    }

    // Shared helper — delegates to DatabaseDescriptor.FireDeferredSchemaStepDownAsync,
    // adding the caller's logger for the step-down failure case. Called from the finally blocks
    // of ExecuteDdlInTransaction, AddColumnAsync, ExecuteStagedAddIndexAsync, and
    // ExecuteIndexDdlAsync so all DDL paths release leadership on degradation.
    private async Task FireDeferredStepDownIfRequestedAsync(DatabaseDescriptor database)
    {
        try
        {
            await database.FireDeferredSchemaStepDownAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            context.Logger.LogError(
                ex,
                "Schema partition step-down after persist exhaustion failed for database {DatabaseName}",
                database.Name
            );
        }
    }

    /// <summary>
    /// Drops an index that an explicit transaction staged and whose build failed. The job record and the
    /// entries stay: the owner's rollback finds the record and removes them. Never throws.
    /// </summary>
    private async Task DropFailedOwnedIndexAsync(DatabaseDescriptor database, string tableName, string indexName)
    {
        try
        {
            if (database.Schema.Tables.TryGetValue(tableName, out TableSchema? tableSchema) &&
                tableSchema.Indexes?.Any(ix => string.Equals(ix.Name, indexName, StringComparison.OrdinalIgnoreCase)) == true)
            {
                await catalogs.ReplicateDropIndexAsync(database, tableName, indexName).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            context.Logger.LogWarning(ex, "Failed to compensate partial index add for {IndexName} on {TableName}", indexName, tableName);
        }
    }

    private static string[] GetColumnIdsForIndex(TableDescriptor table, ReadOnlySpan<ColumnIndexInfo> columns)
    {
        string[] columnIds = new string[columns.Length];
        int i = 0;

        foreach (ColumnIndexInfo columnIndex in columns)
        {
            bool found = false;
            foreach (TableColumnSchema column in table.Schema.Columns!)
            {
                if (!string.Equals(column.Name, columnIndex.Name, StringComparison.OrdinalIgnoreCase))
                    continue;

                if (!SchemaElementStateRules.IsReadable(column) || !SchemaElementStateRules.IsWritable(column))
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Column '{columnIndex.Name}' is not public on table '{table.Name}'"
                    );

                columnIds[i++] = column.Id;
                found = true;
                break;
            }

            if (!found)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Column '{columnIndex.Name}' does not exist on table '{table.Name}'"
                );
        }

        return columnIds;
    }

    /// <summary>
    /// Opens the table an index statement names. For <c>DROP INDEX IF EXISTS</c> a missing table returns
    /// null, which the caller reports as "not applied": an index cannot exist when its table does not.
    /// The miss comes from <see cref="TableOpener"/>, so a stale local catalog is reconciled before it
    /// counts as missing. Every other ticket still fails with <c>TableDoesntExist</c>.
    /// </summary>
    /// <remarks>
    /// Runs after the forward to the schema leader, as <c>DROP TABLE IF EXISTS</c> does, so a follower
    /// with a stale catalog never decides the outcome on its own.
    /// </remarks>
    internal async ValueTask<TableDescriptor?> OpenIndexTableAsync(DatabaseDescriptor database, AlterIndexTicket ticket)
    {
        if (!ticket.IfExists)
            return await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        try
        {
            return await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        }
        catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.TableDoesntExist)
        {
            return null;
        }
    }

    public async Task<bool> AlterIndex(AlterIndexTicket ticket)
    {
        context.Validator.Validate(ticket);

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        bool? forwarded = await ddlForwarding.TryForwardAlterIndexAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return forwarded.Value;

        TableDescriptor? table = await OpenIndexTableAsync(database, ticket).ConfigureAwait(false);
        if (table is null)
            return false;

        // RenameIndex is metadata-only (no KV data changes); use single-phase DDL on all paths.
        if (ticket.Operation == AlterIndexOperation.RenameIndex)
            return await ExecuteDdlInTransaction(database,
                tx => tableIndexAlterer.Alter(queryExecutor, database, table, ticket, tx),
                postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, table.Id),
                tableNames: [ticket.TableName]
            ).ConfigureAwait(false);

        bool addIndexOperation = ticket.Operation is
            AlterIndexOperation.AddIndex or
            AlterIndexOperation.AddUniqueIndex or
            AlterIndexOperation.AddPrimaryKey or
            AlterIndexOperation.AddInvertedIndex;

        if (addIndexOperation && TransactionSchemaScope.OverlayFor(database) is null)
            return await ExecuteStagedAddIndexAsync(database, table, ticket).ConfigureAwait(false);

        return await ExecuteIndexDdlAsync(
            database, table, ticket,
            tx => tableIndexAlterer.Alter(queryExecutor, database, table, ticket, tx)
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Index DDL under the DDL gate, other than an add outside a transaction: the drop of an index or of a
    /// primary key (<see cref="DropIndexStagedWithinGateAsync"/>), and every index statement of a transaction
    /// with a private schema (<see cref="RunIndexDdlInOwnerTransactionAsync"/>). The gate is entered for the
    /// table, which also waits for a staged change in progress on it. A drop takes the semaphore only for
    /// its proposals and holds the table in between, and finds the semaphore held again when it returns.
    ///
    /// <para>Called in both modes, by the SQL dispatcher and by <see cref="AlterIndex"/>. An add outside a
    /// transaction goes through the staged <see cref="ExecuteStagedAddIndexAsync"/> instead, and a rename
    /// through <see cref="ExecuteDdlInTransaction"/>. <paramref name="localWork"/> serves an add inside a
    /// transaction only; a drop has no local work.</para>
    /// </summary>
    internal async Task<bool> ExecuteIndexDdlAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterIndexTicket ticket,
        Func<KvTransaction, Task<bool>> localWork
    )
    {
        if (ticket.Operation is AlterIndexOperation.DropIndex or AlterIndexOperation.DropPrimaryKey
            && TestInterceptBeforeIndexDropGate is { } intercept)
            await intercept().ConfigureAwait(false);

        await database.DdlGate.EnterAsync(ticket.TableName, context.Logger).ConfigureAwait(false);
        try
        {
            return await RunIndexDdlWithinGateAsync(database, table, ticket, localWork).ConfigureAwait(false);
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
            await FireDeferredStepDownIfRequestedAsync(database).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Index DDL inside a transaction with a private schema.
    /// <list type="bullet">
    /// <item>An index add on a table the transaction created runs its local work in the transaction: the
    /// build reads the transaction's own rows and writes their entries in it
    /// (<see cref="IndexBackfiller.BuildInTransactionAsync"/>). Then its delta joins the change set.</item>
    /// <item>An index add on any other table runs the staged build on the shared table, owned by the
    /// transaction (<see cref="StageIndexOnSharedTableAsync"/>).</item>
    /// <item>An index drop only removes the index from the private schema. Its entries stay until the
    /// change set is published, so a ROLLBACK finds them intact. A durable purge record removes them after
    /// the publish (<see cref="DeferredPurgeRecord"/>).</item>
    /// </list>
    /// </summary>
    private async Task<bool> RunIndexDdlInOwnerTransactionAsync(
        DatabaseDescriptor database,
        TransactionSchemaOverlay overlay,
        TableDescriptor table,
        AlterIndexTicket ticket,
        Func<KvTransaction, Task<bool>> localWork)
    {
        KvTransaction tx = overlay.Owner;

        if (ticket.Operation is AlterIndexOperation.DropIndex or AlterIndexOperation.DropPrimaryKey)
        {
            if (!table.AllIndexes.TryGetValue(ticket.IndexName, out TableIndexSchema? dropped))
            {
                if (ticket.IfExists)
                    return false;

                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Index '{ticket.IndexName}' does not exist in table '{table.Name}'");
            }

            await catalogs.ReplicateIndexChangeAsync(database, ticket, table, tx).ConfigureAwait(false);

            // The entries stay until the publish. The record commits with the transaction, and this node
            // starts to remove them right after its publish; the coordinator-job sweep finishes a removal
            // that a crash interrupted. The key space is addressed by storage id and index id, which no
            // rename changes.
            DeferredPurgeRecord purge = overlay.AddDeferredPurge(
                DeferredPurgeKind.IndexEntries, table.Id, table.Schema.EffectiveStorageId, [dropped.KvId]);
            overlay.AddAfterPublish(() =>
            {
                context.DeferredPurges.Schedule(overlay.Database, purge);
                return Task.CompletedTask;
            });

            return true;
        }

        if (!overlay.CreatedTable(table.Id))
            return await StageIndexOnSharedTableAsync(database, overlay, table, ticket).ConfigureAwait(false);

        bool result = await localWork(tx).ConfigureAwait(false);
        if (!result)
            return false;

        await catalogs.ReplicateIndexChangeAsync(database, ticket, table, tx).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// <c>CREATE INDEX</c> inside a transaction with a private schema, on a table the transaction did not
    /// create: the staged build of <see cref="ExecuteStagedAddIndexAsync"/> runs on the shared table, owned
    /// by the transaction, and stops with the index <c>WriteOnly</c> and backfilled. Other transactions
    /// maintain the index from then on but cannot read it. The step to <c>Public</c> joins the change set,
    /// so the publish makes the index readable together with the transaction's data; see
    /// <see cref="RunOwnedStagedChangeAsync"/>. The caller holds the DDL gate, as the staged build requires.
    /// </summary>
    private Task<bool> StageIndexOnSharedTableAsync(
        DatabaseDescriptor database, TransactionSchemaOverlay overlay, TableDescriptor table, AlterIndexTicket ticket)
    {
        // Against the private table first, so a definition that the transaction's own view of the table
        // refuses fails for that reason, also when the staged form cannot run on the table.
        if (BuildStagedIndexInfo(table, ticket, options) is null)
            return Task.FromResult(false);

        return RunOwnedStagedChangeAsync(database, overlay, table, "CREATE INDEX", async () =>
        {
            TableDescriptor shared = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

            if (BuildStagedIndexInfo(shared, ticket, options) is not { } indexInfo)
                return null;

            int steps = await StageIndexWithinGateAsync(database, shared.Id, shared.Name, indexInfo, overlay.ChangeSetId).ConfigureAwait(false);

            return new OwnedStagedElement(shared.Id, shared.Name, steps, [indexInfo.IndexName], [(indexInfo.IndexName, SchemaElementKind.Index)]);
        });
    }

    /// <summary>
    /// What a statement staged on a shared table for an explicit transaction: the table's id and name, the
    /// number of schema versions the staged steps took, the element names of the job records it left (each
    /// owned by the transaction), and the elements whose step to <c>Public</c> joins the change set. A CHECK
    /// or NOT NULL constraint has no such step: it is enforced and proven at the statement.
    /// </summary>
    private sealed record OwnedStagedElement(
        string TableId,
        string TableName,
        int StepsTaken,
        IReadOnlyList<string> OwnedElementNames,
        IReadOnlyList<(string Name, SchemaElementKind Kind)> PublicSteps);

    /// <summary>
    /// Runs a staged change on a shared table for the transaction of <paramref name="overlay"/>, in this
    /// order:
    /// <list type="number">
    /// <item>Refuse the cases the staged form cannot handle (<see cref="TransactionalDdlRules.RequireStageableSharedTable"/>;
    /// <paramref name="locksRows"/> is false for a statement whose check reads with no lock), and a shared
    /// schema that moved under the private one.</item>
    /// <item>Record the removal of every element the transaction stages, once per transaction
    /// (<see cref="SchemaChangeCoordinator.RemoveOwnedElementsOfAsync"/>). It runs at a rollback or an abort,
    /// while the transaction still holds the DDL lock, and reads the job records, so it also finds what a
    /// failure in the next step left behind. Once the lock is free, the coordinator's resume removes an
    /// element whose owner ended without a commit (<see cref="SchemaChangeCoordinator.ResumeOwnedJobAsync"/>).</item>
    /// <item>Run the staged steps on the shared schema, with the private schema out of the flow
    /// (<paramref name="stageOnShared"/>). Every job carries the change set id as its owner, so it stops
    /// before <c>Public</c> and no resume drives it. Null means the statement had nothing to do.</item>
    /// <item>Record the job records for the commit, which deletes them in the transaction's own KV commit
    /// (<see cref="TransactionSchemaOverlay.AddOwnedJob"/>).</item>
    /// <item>Take the shared table over into the private schema
    /// (<see cref="TransactionSchemaOverlay.RebaseOntoSharedTableAsync"/>).</item>
    /// <item>Add the steps to <c>Public</c> to the change set. From here on the transaction reads the
    /// elements; the publish makes them readable to everyone.</item>
    /// </list>
    /// A failure fails the statement and leaves the transaction to roll back, and the rollback removes what
    /// was staged.
    /// </summary>
    private async Task<bool> RunOwnedStagedChangeAsync(
        DatabaseDescriptor database,
        TransactionSchemaOverlay overlay,
        TableDescriptor table,
        string statement,
        Func<Task<OwnedStagedElement?>> stageOnShared,
        bool locksRows = true)
    {
        TransactionalDdlRules.RequireStageableSharedTable(overlay, table, statement, locksRows);
        overlay.RequireSharedBase();

        if (!overlay.OwnedRemovalRecorded)
        {
            string changeSetId = overlay.ChangeSetId;
            SchemaChangeCoordinator remover = context.DatabaseOpener.ResumeCoordinator;
            overlay.AddOnRollback(() => remover.RemoveOwnedElementsOfAsync(database, changeSetId));
            overlay.OwnedRemovalRecorded = true;
        }

        OwnedStagedElement? staged = await RunOnSharedSchemaAsync(stageOnShared).ConfigureAwait(false);
        if (staged is null)
            return false;

        foreach (string elementName in staged.OwnedElementNames)
            overlay.AddOwnedJob(staged.TableId, elementName);

        // The rebase replaces the private copy of the table by id. A name that resolved to another table
        // means a drop and a create landed in between; the rollback then removes what was staged.
        if (!string.Equals(staged.TableId, table.Id, StringComparison.Ordinal))
            throw new CamusDBException(
                CamusDBErrorCodes.ConcurrentSchemaChange,
                $"Table '{table.Name}' was replaced by another statement while the transaction staged a change on it; roll back and retry");

        await overlay.RebaseOntoSharedTableAsync(staged.TableId, staged.StepsTaken).ConfigureAwait(false);

        foreach ((string name, SchemaElementKind kind) in staged.PublicSteps)
            await catalogs.ReplicateElementStateAsync(database, staged.TableName, name, SchemaElementState.Public, kind).ConfigureAwait(false);

        return true;
    }

    /// <summary>
    /// Runs <paramref name="steps"/> against the shared schema: the flow's private schema is cleared for
    /// the length of this call only, because an async method restores the caller's flow value when it
    /// returns.
    /// </summary>
    private static async Task<T> RunOnSharedSchemaAsync<T>(Func<Task<T>> steps)
    {
        TransactionSchemaScope.Suppress();
        return await steps().ConfigureAwait(false);
    }

    /// <summary>
    /// The body of <see cref="ExecuteIndexDdlAsync"/>, for a caller that already holds
    /// <c>SchemaDdlSemaphore</c>: an index statement of a transaction with a private schema
    /// (<see cref="RunIndexDdlInOwnerTransactionAsync"/>), and a drop outside one
    /// (<see cref="DropIndexStagedWithinGateAsync"/>). The foreign-key paths call it for the build of a
    /// constraint's index inside a transaction, and for the drop of an index a removed constraint owned.
    /// </summary>
    private async Task<bool> RunIndexDdlWithinGateAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterIndexTicket ticket,
        Func<KvTransaction, Task<bool>> localWork
    )
    {
        if (TransactionSchemaScope.OverlayFor(database) is { } overlay)
        {
            // Under the DDL semaphore, before the index leaves the private schema. Both the ticket API and
            // the SQL path reach this helper.
            if (ticket.Operation is AlterIndexOperation.DropIndex or AlterIndexOperation.DropPrimaryKey)
                ForeignKeyDependencyRules.RequireIndexNotInForeignKey(database.Schema, table.Schema, ticket.IndexName);

            return await RunIndexDdlInOwnerTransactionAsync(database, overlay, table, ticket, localWork).ConfigureAwait(false);
        }

        if (ticket.Operation is not (AlterIndexOperation.DropIndex or AlterIndexOperation.DropPrimaryKey))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Index '{ticket.IndexName}' of table '{table.Name}' is added by the staged coordinator, not under the DDL gate");

        return await DropIndexStagedWithinGateAsync(database, ticket, table.Id).ConfigureAwait(false);
    }

    /// <summary>
    /// Drops an index outside a transaction, for <c>DROP INDEX</c>, <c>ALTER TABLE ... DROP INDEX</c> and
    /// <c>DROP PRIMARY KEY</c>, in both modes. The caller holds <c>SchemaDdlSemaphore</c>. The coordinator
    /// steps the index down to <c>Absent</c> on every node and then starts the purge of its entries in the
    /// background (<see cref="SchemaChangeCoordinator.RunIndexDropAsync"/>), so the statement does not read
    /// or delete a single entry, and its time and memory do not depend on the size of the index.
    ///
    /// <para><b>Nothing here edits the schema.</b> The table is opened again under the semaphore, so the
    /// existence check, the <c>IF EXISTS</c> answer and the foreign-key refusal see the schema that the
    /// drop starts from; a concurrent drop of the same index makes this one a no-op or a "does not exist".
    /// The index leaves this node's schema only when the node applies the deltas, as on every other node,
    /// so a failure part-way leaves the schema as it is persisted, and the next resume finishes the drop.</para>
    ///
    /// <para><b>The table is the one the caller resolved.</b> The caller opened the table before it entered
    /// the gate, by name. When another statement dropped the table meanwhile, <c>IF EXISTS</c> answers "not
    /// applied" and a plain drop fails with <c>TableDoesntExist</c>. When it also created a table of the same
    /// name, the drop is refused with <c>ConcurrentSchemaChange</c>, as a staged add is, so it never removes an
    /// index of the table that replaced the one the statement named.</para>
    ///
    /// <para><b>The table may be held by the caller.</b> A foreign key whose validation failed drops the
    /// index the engine built for it from inside its own staged job, which holds the table
    /// (<see cref="DropIndexWithinGateAsync"/>). Every other caller entered the gate for the table, and
    /// entering waits for every hold on it, so a hold seen here is the caller's own.</para>
    /// </summary>
    private async Task<bool> DropIndexStagedWithinGateAsync(DatabaseDescriptor database, AlterIndexTicket ticket, string expectedTableId)
    {
        TableDescriptor table;
        try
        {
            table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        }
        catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.TableDoesntExist && ticket.IfExists)
        {
            return false;
        }

        if (!string.Equals(table.Id, expectedTableId, StringComparison.Ordinal))
            throw new CamusDBException(
                CamusDBErrorCodes.ConcurrentSchemaChange,
                $"Table '{ticket.TableName}' was replaced by another statement while index '{ticket.IndexName}' was being dropped; nothing was dropped");

        if (!table.AllIndexes.TryGetValue(ticket.IndexName, out TableIndexSchema? index))
        {
            if (ticket.IfExists)
                return false;

            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Index '{ticket.IndexName}' does not exist in table '{table.Name}'");
        }

        ForeignKeyDependencyRules.RequireIndexNotInForeignKey(database.Schema, table.Schema, index.Name);

        SchemaChangeCoordinator coordinator = NewStagedIndexCoordinator();

        if (TestInterceptAfterIndexDropStep is { } intercept)
            coordinator.OnStepCompleted = (state, _) => intercept(state);

        SchemaChangeJob job = new(database.Name, table.Name, table.Id, index.Name, SchemaElementState.Absent, SchemaElementKind.Index);

        await coordinator.RunIndexDropAsync(
            database, job, index.KvId, table.Schema.EffectiveStorageId,
            gateHeldByCaller: true,
            tableHeldByCaller: database.DdlGate.IsTableHeld(table.Name)).ConfigureAwait(false);

        database.Cache?.InvalidateByTableId(database.Id, table.Id);

        // The deltas evicted the table's descriptor. Opened again here, so a reader of the descriptor
        // cache, such as the statistics and the planners, finds it without the index.
        if (catalogs.TableExists(database, table.Name))
            await context.TableOpener.Open(database, table.Name).ConfigureAwait(false);

        return true;
    }

    /// <summary>
    /// A coordinator for the staged index paths of a statement: the build of an index and its removal. The
    /// purge of a dropped index runs in the background on this node (<see cref="DeferredPurgeRunner.Schedule"/>).
    /// </summary>
    private SchemaChangeCoordinator NewStagedIndexCoordinator() => new(catalogs, context.Logger)
    {
        IndexBackfillAsync = (db, tbl, info, start, checkpoint, cancellationToken) => BackfillIndexEntriesAsync(db, tbl, info, start, checkpoint, cancellationToken),
        PurgeIndexEntriesAsync = (db, storageId, indexId) => context.DeferredPurges.PurgeIndexKeySpaceAsync(db, storageId, indexId),
        SchedulePurge = (db, record) => context.DeferredPurges.Schedule(db, record),
        TestInterceptAfterIndexDropRecorded = TestInterceptBeforeIndexDropFirstStep,
        TestInterceptBeforeIndexDropRecord = TestInterceptBeforeIndexDropRecord,
    };

    /// <summary>
    /// Drops one index, if it still exists, through the staged drop (<see cref="DropIndexStagedWithinGateAsync"/>),
    /// or inside the private schema of the current transaction. Used for the index a foreign key owned,
    /// after that constraint is removed. The caller holds <c>SchemaDdlSemaphore</c>: a statement that
    /// entered the gate for the table, or a staged job of the constraint, which also holds the table. The
    /// resume coordinator calls it from such a job too.
    /// </summary>
    internal async Task DropIndexWithinGateAsync(DatabaseDescriptor database, string tableName, string indexName)
    {
        if (!catalogs.TableExists(database, tableName))
            return;

        TableDescriptor table = await context.TableOpener.Open(database, tableName).ConfigureAwait(false);
        if (!table.AllIndexes.ContainsKey(indexName))
            return;

        AlterIndexTicket ticket = new(database.Name, tableName, indexName, [], AlterIndexOperation.DropIndex, ifExists: true);

        await RunIndexDdlWithinGateAsync(
            database, table, ticket,
            tx => tableIndexAlterer.Alter(queryExecutor, database, table, ticket, tx)
        ).ConfigureAwait(false);
    }

    public async Task<bool> DropTable(DropTableTicket ticket)
    {
        context.Validator.Validate(ticket);

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        bool? forwarded = await ddlForwarding.TryForwardDropTableAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return forwarded.Value;

        if (ticket.IfExists && !catalogs.TableExists(database, ticket.TableName))
            return false;

        TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        return await DropTableWithSequencesAsync(database, table, ticket).ConfigureAwait(false);
    }

    /// <summary>
    /// Refuses <c>DROP TABLE</c> on a materialized view unless the ticket says the caller knows what
    /// it is dropping. A materialized view is stored as a relation, so the table-drop path would
    /// happily remove one; refusing keeps the statement that creates an object and the statement
    /// that removes it symmetric, and matches PostgreSQL.
    /// </summary>
    private static void RequireDroppableAsTable(TableDescriptor table, DropTableTicket ticket)
    {
        if (table.Schema.IsMaterializedView && !ticket.AllowMaterializedView)
            throw new CamusDBException(
                CamusDBErrorCodes.TableDoesntExist,
                $"'{ticket.TableName}' is a materialized view; use DROP MATERIALIZED VIEW");
    }

    /// <summary>
    /// Refuses a drop whose ticket names an <see cref="DropTableTicket.ExpectedTableId"/> when the
    /// opened relation, or the relation the name resolves to now, has a different id. Runs under the
    /// DDL gate: the relation was opened by name before the gate was taken, and a rename in between
    /// can give the name to a different relation.
    /// </summary>
    private static void RequireExpectedRelation(DatabaseDescriptor database, TableDescriptor table, DropTableTicket ticket)
    {
        if (ticket.ExpectedTableId is not { Length: > 0 } expected)
            return;

        if (string.Equals(table.Id, expected, StringComparison.Ordinal)
            && database.Schema.Tables.TryGetValue(ticket.TableName, out TableSchema? current)
            && string.Equals(current.Id, expected, StringComparison.Ordinal))
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.ConcurrentSchemaChange,
            $"'{ticket.TableName}' was renamed or replaced by another statement while it was being dropped; nothing was dropped");
    }

    /// <summary>
    /// Refuses <c>ALTER TABLE … RENAME TO</c> on a materialized view unless the ticket says the caller
    /// knows what it is renaming, naming the statement to use instead — the rename-side twin of
    /// <see cref="RequireDroppableAsTable"/>. Harmless either way today, since both statements go
    /// through the same replicated rename; refused so each kind of object is changed by its own
    /// statement, as PostgreSQL does.
    /// </summary>
    internal static void RequireRenamableAsTable(TableDescriptor table, RenameTableTicket ticket)
    {
        if (table.Schema.IsMaterializedView && !ticket.AllowMaterializedView)
            throw new CamusDBException(
                CamusDBErrorCodes.TableDoesntExist,
                $"'{ticket.TableName}' is a materialized view; use ALTER MATERIALIZED VIEW ... RENAME TO");
    }

    /// <summary>
    /// Drops one table and, when the drop is not deferred, the sequences its identity columns own.
    /// Every entry point into <c>DROP TABLE</c> — the ticket API, the SQL dispatcher, and a statement
    /// forwarded from a follower, which the leader runs through the ticket API — goes through here,
    /// so none of them can be the one that skips a guard or leaks a counter.
    /// </summary>
    /// <remarks>
    /// <para><b>Every rule that can refuse the drop runs first, under the DDL semaphore.</b> The
    /// materialized-view rule, the dependent-view rule and the foreign-key rule all run inside the
    /// DDL transaction before <see cref="TableDropper"/> removes a row or an index entry. The rules
    /// that depend on other relations run again when the drop's delta is applied, in log order.</para>
    ///
    /// <para><b>A deferred drop deliberately keeps the sequences.</b> The relation is detached, not
    /// gone: its rows are retained and <c>RELINK</c> can bring it back, and a table whose identity
    /// column cannot issue is not the table that was dropped. The counters go when the garbage
    /// collector reclaims the orphan.</para>
    ///
    /// <para><b>The table goes first, the counters after.</b> Dropping a counter while the column
    /// that draws from it is still live would make an insert in that window fail on a counter that
    /// is gone; the other order leaves, at worst, a counter nothing names, which the whole-database
    /// purge sweeps.</para>
    /// </remarks>
    internal async Task<bool> DropTableWithSequencesAsync(
        DatabaseDescriptor database, TableDescriptor table, DropTableTicket ticket)
    {
        // Read before the drop: afterwards the relation is out of the schema and there is nothing
        // left to match an owned sequence against.
        bool deferred = !ticket.Force && database.Ancestors.Count == 0;

        List<string> ownedSequences = deferred
            ? []
            : CollectOwnedSequenceNames(database, table.Id);

        bool dropped = await ExecuteDdlInTransaction(database,
            tx =>
            {
                // Under the DDL gate and before the dropper removes any row or index entry, so a
                // refusal costs nothing to undo. The ticket API, the SQL path and a forwarded drop on
                // the leader all reach this point.
                RequireDroppableAsTable(table, ticket);
                RequireExpectedRelation(database, table, ticket);
                ViewDependencyRules.RequireNotReadByViews(database.Schema, table.Schema);
                ForeignKeyDependencyRules.RequireNotReferencedByOtherTables(database.Schema, table.Schema, "drop table");

                return tableDropper.Drop(queryExecutor, rowDeleter, database, table, ticket, tx);
            },
            postCommitInvalidate: () =>
            {
                database.Cache?.InvalidateByTableId(database.Id, table.Id);

                // The drop can have replaced the job of an unfinished DROP INDEX with its purge record
                // (CoordinatorJobStore.DeleteCoordinatorJobsForTableAsync); the next purge run looks for it.
                database.MarkDeferredPurgesDue();
            },
            tableNames: [table.Name]
        ).ConfigureAwait(false);

        if (dropped)
            await DropOwnedSequencesAsync(database, ownedSequences).ConfigureAwait(false);

        return dropped;
    }

    /// <summary>The names of every sequence owned by a column of <paramref name="tableId"/>.</summary>
    internal static List<string> CollectOwnedSequenceNames(DatabaseDescriptor database, string tableId)
    {
        List<string> owned = [];

        foreach (SequenceSchema sequence in database.Schema.Sequences.Values)
        {
            if (string.Equals(sequence.OwnedByTableId, tableId, StringComparison.Ordinal) && sequence.Name is { Length: > 0 } name)
                owned.Add(name);
        }

        return owned;
    }

    /// <summary>
    /// Removes the sequences an owner took with it.
    /// </summary>
    /// <remarks>
    /// Best effort by design: the owner is already gone, so failing the statement now would report
    /// an error for work that succeeded. A counter left behind is unreachable — nothing in the
    /// catalog names its id any more — and the whole-database purge sweeps it, so the cost of a
    /// failure here is bounded storage rather than incorrect behavior.
    /// </remarks>
    internal async Task DropOwnedSequencesAsync(DatabaseDescriptor database, IReadOnlyList<string> sequenceNames)
    {
        foreach (string sequenceName in sequenceNames)
        {
            try
            {
                await sequenceDdl.DropByNameAsync(database, sequenceName, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                context.Logger.LogWarning(
                    ex,
                    "The owner of sequence '{Sequence}' in database {DatabaseName} was dropped, but the sequence " +
                    "could not be removed; it is unreachable and will be swept with the database",
                    sequenceName,
                    database.Name);
            }
        }
    }

    /// <summary>
    /// Empties a base table by replacing the physical key-space its rows and index entries live in.
    /// </summary>
    /// <remarks>
    /// <para><b>The cost does not grow with the row count</b>, because no row and no index entry is
    /// read or deleted. What the statement waits for instead is the exclusive fence over the old row
    /// key-space, the replicated schema entry, and the checkpoint that makes the retired generation
    /// recoverable — so it is constant in cardinality, not constant in wall-clock time.</para>
    ///
    /// <para><b>Order is the whole design.</b> The fence is taken <em>before</em> the cut timestamp is
    /// minted and before anything is proposed. Kahuna never grants the fence over a write another
    /// transaction — optimistic or pessimistic — staged into the old key-space: the fence transaction
    /// holds nothing at that point, so it waits for such a writer to finish (up to the lock-wait
    /// deadline) rather than being refused, and once it holds the fence every later writer into the
    /// old key-space is refused. That is what stops a writer from having a row acknowledged into
    /// storage nothing reads afterwards, and it is what makes the cut a real linearization point
    /// rather than a hopeful one.</para>
    ///
    /// <para><b>Once the schema entry commits, the truncate has happened.</b> It is cluster-wide and
    /// irrevocable at that point, so a later failure to finalize the fence-owning transaction is
    /// logged and the committed outcome is reported. Reporting a rollback there would tell the caller
    /// the table still holds rows it no longer holds.</para>
    /// </remarks>
    public async Task<bool> TruncateTable(TruncateTableTicket ticket)
    {
        context.Validator.Validate(ticket);

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        bool? forwarded = await ddlForwarding.TryForwardTruncateTableAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return forwarded.Value;

        TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        RequireTruncatableRelation(table);
        RequireTruncatePrivileges(database, table);

        // Allocated before the DDL transaction, like a table id on CREATE: only the leader allocates,
        // and the value rides the replicated payload so every node adopts the same key-space.
        DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);
        string newStorageId = await registry.AllocateTableIdAsync().ConfigureAwait(false);

        await database.DdlGate.EnterAsync(ticket.TableName, context.Logger).ConfigureAwait(false);

        KvTransaction fenceTx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            mutationLimitOverride: 0
        ).ConfigureAwait(false);

        bool deltaCommitted = false;
        try
        {
            // Under the DDL gate, so a child created by a concurrent CREATE TABLE is seen. The apply
            // checks it again in log order.
            ForeignKeyDependencyRules.RequireNotReferencedByOtherTables(database.Schema, table.Schema, "truncate");

            // Both index lists are read under the DDL gate, not before it. An ADD INDEX that landed
            // between the read and the proposal would leave its entries out of the retired
            // generation's frozen catalog — unnamed, and so never reclaimed — and out of the new
            // generation's seeded one. The gate is what excludes it: index DDL takes the same
            // semaphore, and cluster-wide it serializes through this node as the schema leader.
            //
            // The retired list comes from the persisted catalog rather than the live index list,
            // because the retired generation owns entries for every index it ever had, and one
            // dropped before the truncate is named nowhere else. Reading it before the fence
            // transaction stages anything keeps the read off its own write intents.
            string[] retiredIndexIds = await catalogs.ReadStorageIndexCatalogAsync(database, table.Schema).ConfigureAwait(false);

            string[] newIndexIds = table.Schema.Indexes is null
                ? []
                : [.. table.Schema.Indexes.Where(index => !string.IsNullOrEmpty(index.Id)).Select(index => index.Id!)];

            await table.Store.AcquireExclusiveRowSpaceFenceAsync(fenceTx).ConfigureAwait(false);

            // Re-read under the fence. Between resolving the descriptor and holding the fence another
            // contents change may have landed; proposing the values read earlier would ask apply to
            // swap a generation this statement never saw, and apply would refuse it anyway.
            string expectedStorageId = table.Schema.EffectiveStorageId;
            long expectedGeneration = table.Schema.ContentsGeneration;

            HLCTimestamp cut = context.SharedNode is null
                ? fenceTx.TransactionId
                : context.SharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(context.SharedNode.Raft.GetLocalNodeId());

            await catalogs.TruncateTableContentsAsync(
                database,
                table.Id,
                table.Name,
                expectedStorageId,
                expectedGeneration,
                newStorageId,
                cut,
                retiredIndexIds,
                newIndexIds
            ).ConfigureAwait(false);

            deltaCommitted = true;

            using CancellationTokenSource cts = new(DdlCommitTimeout);
            await database.Transactions.CommitAsync(fenceTx, cts.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            await database.Transactions.RollbackIfNotCompletedAsync(fenceTx).ConfigureAwait(false);

            if (!deltaCommitted)
                throw;

            context.Logger.LogWarning(
                ex,
                "TRUNCATE of table {Table} in database {Database} committed its schema entry but failed to " +
                "finalize the fence-owning transaction; the truncate stands and the fence expires on its own",
                table.Name, database.Name);
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
            await FireDeferredStepDownIfRequestedAsync(database).ConfigureAwait(false);
        }

        database.Cache?.InvalidateByTableId(database.Id, table.Id);

        if (ticket.RestartIdentity)
            await RestartOwnedSequencesAsync(database, table.Id, ticket.TableName).ConfigureAwait(false);

        return true;
    }

    /// <summary>
    /// Returns every sequence owned by a column of <paramref name="tableId"/> to its recorded start
    /// value, after the relation's contents have been replaced.
    /// </summary>
    /// <remarks>
    /// <para><b>Owned sequences only.</b> A sequence a column merely defaults from is shared —
    /// other relations may draw from it — and resetting one would be a data-loss-shaped surprise
    /// for tables this statement never named. That is also PostgreSQL's rule.</para>
    ///
    /// <para><b>Ownership is keyed on the relation id, not the storage id.</b> A truncate is
    /// exactly where the two come apart: the relation keeps its identity and swaps the key-space
    /// its rows live in, so a sequence matched by storage id would be missed by every truncate
    /// after the first.</para>
    ///
    /// <para><b>The restart runs after the swap has committed, and a failure is reported to the
    /// caller.</b> The contents swap cannot be undone, so the statement does not fail as a whole —
    /// but it does not report plain success either: the caller asked for two things and got one,
    /// and a silent partial result is how a reset that never happened is discovered from the data
    /// weeks later. A sequence that kept climbing is safe in itself, because it issues values above
    /// everything the relation ever held.</para>
    /// </remarks>
    private async Task RestartOwnedSequencesAsync(DatabaseDescriptor database, string tableId, string tableName)
    {
        List<string> notRestarted = [];

        foreach (SequenceSchema sequence in database.Schema.Sequences.Values)
        {
            if (!string.Equals(sequence.OwnedByTableId, tableId, StringComparison.Ordinal))
                continue;

            try
            {
                await sequenceDdl.SetCounterAsync(
                    database, sequence, sequence.StartValue, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                context.Logger.LogWarning(
                    ex,
                    "TRUNCATE ... RESTART IDENTITY emptied a relation in database {Database} but could not restart " +
                    "sequence '{Sequence}'",
                    database.Name, sequence.Name);

                notRestarted.Add(sequence.Name ?? sequence.Id!);
            }
        }

        if (notRestarted.Count == 0)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.SequenceUnavailable,
            $"TRUNCATE emptied '{tableName}', and that part is committed and final — but these sequences were " +
            $"not restarted: {string.Join(", ", notRestarted)}. They keep climbing, which is safe (every value " +
            "they issue is above anything the relation held); re-run ALTER SEQUENCE ... RESTART on each to " +
            "finish what the statement asked for.");
    }

    /// <summary>
    /// Refuses a <c>TRUNCATE</c> whose target is not an ordinary base table.
    /// </summary>
    /// <remarks>
    /// A materialized view is stored as a relation and already uses the same storage-swap machinery
    /// for its own refresh, so a truncate would contend with the refresh path for one field and would
    /// retire the view's contents outside the refresh's lease and fence. A plain view never reaches
    /// here: resolving one for a write already reports that it is a view.
    /// </remarks>
    private static void RequireTruncatableRelation(TableDescriptor table)
    {
        if (!table.Schema.IsMaterializedView)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.ViewNotUpdatable,
            $"'{table.Name}' is a materialized view and cannot be truncated; its contents come from its " +
            $"defining query. Use REFRESH MATERIALIZED VIEW {table.Name} WITH NO DATA to empty it.");
    }

    /// <summary>
    /// Requires DELETE <b>and</b> DROP on the target.
    /// </summary>
    /// <remarks>
    /// <para>The two are checked one at a time on purpose. A privilege test asks whether a single
    /// grant record carries the whole requested mask, so <c>Delete | Drop</c> as one mask refuses a
    /// principal who was granted DELETE in one statement and DROP in another — a perfectly ordinary
    /// way to hold both.</para>
    ///
    /// <para>DELETE is also the privilege the per-table chokepoint checks while resolving the table,
    /// so on a request that carries a caller it is verified twice. That is cheap, and it keeps the two
    /// checks reading the same ambient scope.</para>
    ///
    /// <para><b>An absent scope is not an unauthenticated caller.</b> It means no request-level entry
    /// point published one — an internal caller, or the schema leader running a statement a follower
    /// already authorized before forwarding it. The per-table chokepoint treats that the same way, and
    /// diverging here would fail every forwarded truncate on an authenticated cluster.</para>
    /// </remarks>
    private static void RequireTruncatePrivileges(DatabaseDescriptor database, TableDescriptor table)
    {
        if (!database.Options.AuthenticationEnabled)
            return;

        AuthorizationScope scope = AuthorizationContext.Current;
        if (scope.Principal is not { } principal || scope.RequiredPrivilege is null)
            return;

        foreach (Privilege required in TruncateRequiredPrivileges)
        {
            if (!principal.HasPrivilege(required, database.Id, table.Id))
                throw new CamusDBException(
                    CamusDBErrorCodes.InsufficientPrivilege,
                    $"Missing {required} privilege on table '{database.Name}.{table.Name}'");
        }
    }

    /// <summary>Both privileges <c>TRUNCATE</c> requires, checked independently. Never OR them.</summary>
    private static readonly Privilege[] TruncateRequiredPrivileges = [Privilege.Delete, Privilege.Drop];

    /// <summary>
    /// Recovers an orphaned (deferred-dropped) table by reattaching it to the schema under a new name,
    /// reusing its preserved id and retained row/index data. Like other table DDL it forwards to the
    /// schema leader in cluster mode. Fails with <see cref="CamusDBErrorCodes.OrphanNotFound"/> if no
    /// orphan record exists for the id, or <see cref="CamusDBErrorCodes.TableAlreadyExists"/> if the new
    /// name is taken.
    /// </summary>
    public async Task<bool> RelinkTable(RelinkTableTicket ticket)
    {
        CamusDBOptions currentOptions = options;

        context.Validator.Validate(ticket);

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        bool? forwarded = await ddlForwarding.TryForwardRelinkTableAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return forwarded.Value;

        // Fence this table id against a concurrent GC reclamation (and a second relink). The reclaimer
        // takes the same per-table drop-intent key before purging, so the two never interleave. All the
        // state decisions below happen under this fence.
        DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);
        string fenceId = RegistryDropMarkers.TableFenceId(database.Id, ticket.OrphanTableId);
        bool fenced = await registry.DropMarkers.AcquireDropIntentAsync(fenceId).ConfigureAwait(false);
        if (!fenced)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"A concurrent operation on orphan table id '{ticket.OrphanTableId}' is in progress; retry later");

        try
        {
            // Load the record first: retired contents and a dropped relation are recovered differently,
            // and the liveness tests below are only meaningful for a dropped relation. For retired
            // contents the source relation is normally still live at this very id.
            OrphanTableRecord? record = await catalogs.TryGetTableOrphanAsync(database, ticket.OrphanTableId).ConfigureAwait(false);

            if (record is { Kind: OrphanKind.RetiredContents })
                return await RelinkRetiredContentsAsync(database, record, ticket, currentOptions).ConfigureAwait(false);

            // Idempotency by id: if a live table already has this id — a relink that committed its apply
            // but crashed before deleting the orphan record — don't reattach a second alias.
            TableSchema? liveWithId = null;
            foreach (TableSchema t in database.Schema.Tables.Values)
                if (string.Equals(t.Id, ticket.OrphanTableId, StringComparison.Ordinal)) { liveWithId = t; break; }

            if (liveWithId is not null)
            {
                // The first contents of a materialized view live under the view's own id, so the
                // contents a refresh replaced can have the id the live view still uses. Nothing can
                // reattach them under a second name.
                if (record is { ReplacedMaterializedViewContents: true })
                    throw new CamusDBException(
                        CamusDBErrorCodes.OrphanNotFound,
                        $"Orphan id '{ticket.OrphanTableId}' holds contents that a refresh of materialized view " +
                        $"'{liveWithId.Name}' replaced, and the view still uses that id, so they cannot be relinked");

                if (!string.Equals(liveWithId.Name, ticket.NewTableName, StringComparison.OrdinalIgnoreCase))
                    throw new CamusDBException(
                        CamusDBErrorCodes.TableAlreadyExists,
                        $"Table id '{ticket.OrphanTableId}' is already live under name '{liveWithId.Name}'");

                // Already relinked to this exact name — finish idempotently by cleaning any stale record.
                await DeleteTableOrphanRecordAsync(database, ticket.OrphanTableId).ConfigureAwait(false);
                return true;
            }

            if (catalogs.TableExists(database, ticket.NewTableName))
                throw new CamusDBException(
                    CamusDBErrorCodes.TableAlreadyExists,
                    $"Table '{ticket.NewTableName}' already exists");

            OrphanTableRecord? orphan = record;
            if (orphan is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.OrphanNotFound,
                    $"No orphaned table with id '{ticket.OrphanTableId}' is available to relink in database '{ticket.DatabaseName}'");

            // Relink adds a live table, so it counts against the per-database table limit like CREATE.
            int maxTables = currentOptions.MaxTablesPerDatabase;
            if (maxTables > 0 && database.Schema.Tables.Count >= maxTables)
                throw new CamusDBException(
                    CamusDBErrorCodes.SchemaLimitExceeded,
                    $"Database '{ticket.DatabaseName}' would exceed the maximum of {maxTables} tables per database");

            await ExecuteDdlInTransaction(database,
                tx => catalogs.RelinkTable(database, orphan, ticket.NewTableName, tx),
                postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, orphan.TableId),
                tableNames: [ticket.NewTableName]
            ).ConfigureAwait(false);
        }
        finally
        {
            await registry.DropMarkers.ReleaseDropIntentAsync(fenceId).ConfigureAwait(false);
        }

        return true;
    }

    /// <summary>
    /// Recovers one retired contents generation as a new relation, under the caller's fence.
    /// </summary>
    /// <remarks>
    /// <para>Resumable at every crash point. The target id is written into the record before the
    /// relation is published, so a retry finishes what a crash interrupted instead of allocating a
    /// second target; and a target that is already live with the retired storage means the recovery
    /// completed and only the record was left behind.</para>
    /// </remarks>
    private async Task<bool> RelinkRetiredContentsAsync(
        DatabaseDescriptor database, OrphanTableRecord record, RelinkTableTicket ticket, CamusDBOptions currentOptions)
    {
        TableSchema? existingTarget = null;
        if (record.RelinkTargetId is { Length: > 0 } recordedTarget)
        {
            foreach (TableSchema live in database.Schema.Tables.Values)
            {
                if (string.Equals(live.Id, recordedTarget, StringComparison.Ordinal))
                {
                    existingTarget = live;
                    break;
                }
            }
        }

        if (existingTarget is not null)
        {
            // The relation was already published for this record. Finish idempotently rather than
            // publishing a second one; the record is the only thing still outstanding.
            if (!string.Equals(existingTarget.Name, ticket.NewTableName, StringComparison.OrdinalIgnoreCase))
                throw new CamusDBException(
                    CamusDBErrorCodes.TableAlreadyExists,
                    $"The retired contents of table id '{ticket.OrphanTableId}' were already recovered as '{existingTarget.Name}'");

            await DeleteTableOrphanRecordAsync(database, record.RetiredStorageId).ConfigureAwait(false);
            return true;
        }

        if (catalogs.TableExists(database, ticket.NewTableName))
            throw new CamusDBException(
                CamusDBErrorCodes.TableAlreadyExists,
                $"Table '{ticket.NewTableName}' already exists");

        int maxTables = currentOptions.MaxTablesPerDatabase;
        if (maxTables > 0 && database.Schema.Tables.Count >= maxTables)
            throw new CamusDBException(
                CamusDBErrorCodes.SchemaLimitExceeded,
                $"Database '{ticket.DatabaseName}' would exceed the maximum of {maxTables} tables per database");

        // A recorded target that is not live means a previous attempt crashed before publishing.
        // Reuse its id so the retry converges instead of leaving a second allocated id behind.
        DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);
        string targetId = record.RelinkTargetId is { Length: > 0 } resumed
            ? resumed
            : await registry.AllocateTableIdAsync().ConfigureAwait(false);

        // Durable before the relation exists: this is the only signal that separates a recovery in
        // progress from one that never started, and the collector must not purge the storage in between.
        await catalogs.RecordRelinkTargetAsync(database, record, targetId).ConfigureAwait(false);

        await ExecuteDdlInTransaction(database,
            tx => catalogs.RelinkRetiredContents(database, record, targetId, ticket.NewTableName, tx),
            postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, targetId),
            tableNames: [ticket.NewTableName]
        ).ConfigureAwait(false);

        return true;
    }

    /// <summary>Deletes a stale table orphan record in its own committed transaction (idempotent cleanup).</summary>
    private async Task DeleteTableOrphanRecordAsync(DatabaseDescriptor database, string tableId)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            await catalogs.DeleteTableOrphanAsync(database, tableId, tx).ConfigureAwait(false);
            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    /// <param name="dependentViews">
    /// Rewritten bodies for views that read this relation, applied in the rename's own delta. When
    /// null they are computed here; a caller that already resolved them (the materialized-view rename
    /// path) passes them so the work is not repeated.
    /// </param>
    public async Task<bool> RenameTable(
        RenameTableTicket ticket,
        Dictionary<string, ViewDefinition>? dependentViews = null)
    {
        context.Validator.Validate(ticket);

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        bool? forwarded = await ddlForwarding.TryForwardRenameTableAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return forwarded.Value;

        TableDescriptor renameTable = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        RequireRenamableAsTable(renameTable, ticket);

        dependentViews ??= ViewDependencyMaintainer.BuildRenameConversions(
            database.Schema, renameTable.Id, sql => SQLParserProcessor.Parse(sql, sqlParserCache));

        return await ExecuteDdlInTransaction(database,
            tx => catalogs.RenameTable(database, ticket, tx, dependentViews),
            postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, renameTable.Id),
            tableNames: [ticket.TableName, ticket.NewName]
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Opens a table by <b>database name</b>, resolving the database through the registry first.
    ///
    /// <para>For request-scoped callers that only have a name. Code that already holds a
    /// <see cref="DatabaseDescriptor"/> must call <see cref="OpenTableWithDescriptor"/> instead —
    /// passing <c>descriptor.Name</c> back into this method re-resolves a cached display name, which
    /// after a RENAME DATABASE was the pre-rename name and made every INSERT fail with
    /// "Database '&lt;old&gt;' does not exist". It is also two redundant lookups on a hot path.</para>
    /// </summary>
    public async Task<TableDescriptor> OpenTable(OpenTableTicket ticket)
    {
        DatabaseDescriptor descriptor = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = descriptor.Use();
        return await context.TableOpener.Open(descriptor, ticket.TableName).ConfigureAwait(false);
    }

    /// <summary>
    /// Opens a table against an already-resolved database. The preferred entry point whenever the
    /// caller holds a descriptor: it skips the registry round-trip and cannot be affected by a rename
    /// that happened after the descriptor was cached. <c>ticket.DatabaseName</c> is carried for
    /// diagnostics only — the descriptor decides which database is used.
    /// </summary>
    public async Task<TableDescriptor> OpenTableWithDescriptor(DatabaseDescriptor descriptor, OpenTableTicket ticket)
    {
        return await context.TableOpener.Open(descriptor, ticket.TableName).ConfigureAwait(false);
    }

    /// <summary>
    /// Adds or drops a constraint on an existing table: CHECK, named NOT NULL, or a foreign key (see
    /// <see cref="AlterConstraintLocalAsync"/>). For ADD CHECK, scans all existing rows and rejects if
    /// any row violates the expression. For ADD FOREIGN KEY, runs the staged rollout.
    /// </summary>
    public async Task<ExecuteDDLSQLResult> AlterConstraint(AlterConstraintTicket ticket)
    {
        context.Validator.Validate(ticket);

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        // Before the forward: a forwarded statement runs on the leader with no user principal.
        ForeignKeyPrivileges.RequireReferencePrivileges(database, ticket);

        bool? forwarded = await ddlForwarding.TryForwardAlterConstraintAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return new ExecuteDDLSQLResult(database, forwarded.Value);

        TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        bool ok = await AlterConstraintLocalAsync(database, table, ticket).ConfigureAwait(false);
        database.Cache?.InvalidateByTableId(database.Id, table.Id);
        return new ExecuteDDLSQLResult(database, ok);
    }

    /// <summary>
    /// Attaches or removes a comment on a table, column, index, or database. Public so a ticket caller
    /// can invoke it without going through SQL. The database target is routed to the registry; the
    /// other three open the table and go through <see cref="CommentSetter"/>.
    /// </summary>
    public async Task<ExecuteDDLSQLResult> Comment(CommentTicket ticket)
    {
        context.Validator.Validate(ticket);

        if (ticket.Target == CommentTarget.Database)
        {
            await CommentDatabase(ticket).ConfigureAwait(false);

            // No descriptor to hand back — the registry write needs no open database — but the
            // operation did succeed, so report that rather than a defaulted (false) result.
            return new ExecuteDDLSQLResult(null!, true);
        }

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();
        return await Comment(database, ticket).ConfigureAwait(false);
    }

    /// <summary>
    /// Applies a table/column/index comment against an already-opened database, forwarding to the
    /// schema leader first when this node is a follower. Serialized against other DDL on this
    /// database by the DDL gate, exactly like <c>ALTER TABLE … SET</c>.
    /// </summary>
    internal async Task<ExecuteDDLSQLResult> Comment(DatabaseDescriptor database, CommentTicket ticket)
    {
        bool? forwarded = await ddlForwarding.TryForwardCommentAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return new ExecuteDDLSQLResult(database, forwarded.Value);

        // The table is opened INSIDE the gate, not before it. Resolving first and validating later
        // is a check-then-act: a drop-and-recreate of the same name in between leaves the validation
        // looking at the old object while the delta — which names its target by table name, not id —
        // lands on the replacement. The apply deliberately no-ops on a missing column/index (for
        // replay safety), so that mismatch would report success while doing nothing at all.
        await database.DdlGate.EnterAsync(ticket.TableName!, context.Logger).ConfigureAwait(false);
        try
        {
            TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName!).ConfigureAwait(false);

            // Inside a transaction with a private schema the comment goes through a delta, which joins the
            // change set. The single-node writer changes the schema in place and persists it at once.
            bool replicated = context.IsClusterMode || TransactionSchemaScope.OverlayFor(database) is not null;
            bool ok = await commentSetter.Set(catalogs, database, table, ticket, replicated).ConfigureAwait(false);
            return new ExecuteDDLSQLResult(database, ok);
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
        }
    }

    /// <summary>
    /// Sets a database's comment on the cross-database registry. No schema-leader forwarding: the
    /// registry write is a plain replicated KV write, exactly like RENAME DATABASE.
    /// </summary>
    internal async Task CommentDatabase(CommentTicket ticket)
    {
        DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);
        await registry.SetCommentAsync(ticket.DatabaseName, ticket.Comment).ConfigureAwait(false);
    }
}
