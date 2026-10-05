
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Catalogs;

/// <summary>
/// Drives a schema element through a staged online-schema-change sequence one adjacent
/// <c>SetElementState</c> transition at a time, waiting for every live cluster node to
/// ack the committed version before emitting the next delta.
///
/// <para>
/// This implements the <em>two-version invariant</em> (the architecture documentation) for
/// multi-step sequences: after each step all nodes are on the same version before the
/// coordinator advances to the next, so the cluster never has more than two adjacent
/// schema versions in flight simultaneously.
/// </para>
///
/// <para>
/// The coordinator is stateless beyond the job description; persistence enables leader-change
/// resume.  It must be called on the schema leader — followers should
/// forward DDL via the production HTTP path rather than running the
/// coordinator directly.
/// </para>
/// </summary>
public sealed class SchemaChangeCoordinator
{
    // Canonical ordering used to compute the transition path between any two states.
    private static readonly SchemaElementState[] StateOrder =
    [
        SchemaElementState.Absent,
        SchemaElementState.DeleteOnly,
        SchemaElementState.WriteOnly,
        SchemaElementState.Public,
    ];

    private readonly CatalogsManager catalogs;

    private readonly ILogger<ICamusDB>? logger;

    /// <summary>
    /// Optional callback invoked after each step completes and the ack gate has
    /// been crossed.  Receives the state just reached and the resulting schema
    /// version so tests can assert intermediate cluster state before proceeding.
    /// </summary>
    public Func<SchemaElementState, long, Task>? OnStepCompleted { get; set; }

    /// <summary>
    /// Optional delegate invoked once, just before a column transitions from
    /// <c>WriteOnly</c> to <c>Public</c>. Fires on both the initial run and any
    /// leader-change resume that starts from <c>WriteOnly</c>, closing the crash window.
    /// Must be set on the command-path coordinator and on the resume coordinator in
    /// <c>DatabaseOpener</c>.
    /// </summary>
    public Func<DatabaseDescriptor, string, ColumnInfo, Task>? BackfillAsync { get; set; }

    /// <summary>
    /// Optional delegate invoked once, just before an index transitions from
    /// <c>WriteOnly</c> to <c>Public</c>. Receives the database, table name, index build
    /// info, the last committed backfill offset (null = start from beginning), and a
    /// checkpoint callback the implementation must invoke after each committed batch with
    /// the last processed rowId so a leader-change resume can skip already-indexed rows.
    /// Idempotent: using <c>backfillMode: true</c> in <c>PutIndexEntry</c> ensures re-runs
    /// on resume are safe even for unique indexes.
    /// </summary>
    public Func<DatabaseDescriptor, string, IndexBuildInfo, string?, Func<string, Task>?, Task>? IndexBackfillAsync { get; set; }

    /// <summary>
    /// Delegate invoked once, just before a foreign key transitions from <c>WriteOnly</c> to
    /// <c>Public</c>. Receives the database, the child table name and the constraint name, and throws
    /// <see cref="CamusDBErrorCodes.ForeignKeyViolation"/> when a row has no parent. It runs after
    /// every live node acked <c>WriteOnly</c> and settled the transactions that wrote either table
    /// before that (<see cref="RequireEarlierWritersSettledAsync"/>). So every write since then is
    /// enforced, no write from before can still commit, and the pass needs no lock; any orphan it
    /// finds was committed in the window before the ack.
    ///
    /// <para>On a violation the coordinator removes the constraint (<c>WriteOnly → Absent</c>), deletes
    /// the job and rethrows, so the DDL fails and nothing is left half-rolled-out. It must be set on the
    /// command-path coordinator and on the resume coordinator in <c>DatabaseOpener</c>; without it a
    /// foreign key would be published unvalidated.</para>
    /// </summary>
    public Func<DatabaseDescriptor, string, string, Task>? ForeignKeyValidationAsync { get; set; }

    /// <summary>
    /// Delegate that drops one index of a table: its entries and its schema entry. Called after a
    /// foreign key that failed validation was removed, for the index the engine had built for that
    /// constraint (<see cref="TableIndexSchema.OwnerConstraintId"/>). Receives the database, the table
    /// name and the index name. Without it, the coordinator drops only the schema entry, and the
    /// entries stay in storage, unreachable.
    /// </summary>
    public Func<DatabaseDescriptor, string, string, Task>? DropIndexAsync { get; set; }

    /// <summary>
    /// Reads every row of a table against a CHECK or a NOT NULL constraint, by table name, element kind
    /// (<see cref="SchemaElementKind.Check"/> or <see cref="SchemaElementKind.NotNull"/>) and constraint
    /// name. It throws <see cref="CamusDBErrorCodes.CheckConstraintViolation"/> or
    /// <see cref="CamusDBErrorCodes.NotNullViolation"/> for a row that breaks the constraint, and
    /// returns when the constraint no longer exists. Required by
    /// <see cref="ValidateRowConstraintAsync"/>, and by a resume that finds such a job.
    /// </summary>
    public Func<DatabaseDescriptor, string, SchemaElementKind, string, Task>? RowConstraintValidationAsync { get; set; }

    public SchemaChangeCoordinator(CatalogsManager catalogs, ILogger<ICamusDB>? logger = null)
    {
        this.catalogs = catalogs;
        this.logger = logger;
    }

    /// <summary>
    /// Advances the named column element from its current state toward
    /// <see cref="SchemaChangeJob.TargetState"/>, one adjacent transition at a time.
    ///
    /// <para>
    /// When the column does not yet exist (state = <c>Absent</c>) and the target
    /// requires an add sequence, <paramref name="columnDefinition"/> is used to
    /// create the column in <c>DeleteOnly</c> as the first step.  A null
    /// <paramref name="columnDefinition"/> is accepted when the column already
    /// exists (state transitions only).
    /// </para>
    ///
    /// </summary>
    public async Task RunJobAsync(
        DatabaseDescriptor database,
        SchemaChangeJob job,
        ColumnInfo? columnDefinition = null,
        IndexBuildInfo? indexBuildInfo = null,
        CancellationToken cancellationToken = default
    )
    {
        SchemaElementState current = GetCurrentElementState(database.Schema, job.TableName, job.ElementName, job.ElementKind);
        SchemaElementState[] path = ComputePathFor(job.ElementKind, current, job.TargetState);

        if (path.Length == 0)
            return;

        // Persist the job (attempt 0) so a new leader can resume if this coordinator is
        // interrupted. ResumeJobsAsync bumps the attempt count on each pickup and abandons the
        // job once it exhausts the retry budget, so a doomed job can't loop forever.
        await catalogs.PersistCoordinatorJobAsync(
            database, BuildPersistedJob(job, columnDefinition, indexBuildInfo, attempts: 0)
        ).ConfigureAwait(false);

        await DriveToTargetAsync(database, job, columnDefinition, indexBuildInfo, startOffset: null, current, path, currentAttempts: 0, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Runs the adjacent-transition steps in <paramref name="path"/>, gating on the cluster-wide
    /// ack of each version (enforced inside the <c>Replicate*</c> methods). Deletes the durable
    /// job record only when the element actually reaches the target; a transient failure (e.g.
    /// leadership loss) leaves the record so a new leader can resume it.
    /// </summary>
    private async Task DriveToTargetAsync(
        DatabaseDescriptor database,
        SchemaChangeJob job,
        ColumnInfo? columnDefinition,
        IndexBuildInfo? indexBuildInfo,
        string? startOffset,
        SchemaElementState current,
        SchemaElementState[] path,
        int currentAttempts,
        CancellationToken cancellationToken
    )
    {
        try
        {
            foreach (SchemaElementState nextState in path)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Backfill existing rows just BEFORE the element becomes Public.
                // `current` is still the PRIOR state (not yet reassigned), so this fires
                // exactly on the WriteOnly → Public transition — both on initial run and
                // on a leader-change resume that starts from WriteOnly.
                if (current == SchemaElementState.WriteOnly && nextState == SchemaElementState.Public)
                {
                    await RequireEarlierWritersSettledAsync(database, job).ConfigureAwait(false);

                    if (job.ElementKind == SchemaElementKind.Column &&
                        BackfillAsync is not null &&
                        columnDefinition is not null)
                    {
                        await BackfillAsync(database, job.TableName, columnDefinition).ConfigureAwait(false);
                    }
                    else if (job.ElementKind == SchemaElementKind.Index &&
                             IndexBackfillAsync is not null &&
                             indexBuildInfo is not null)
                    {
                        // Build a checkpoint callback: after each committed batch, persist the
                        // last processed rowId so a leader-change resume skips already-indexed rows.
                        Func<string, Task> checkpoint = async offset =>
                        {
                            PersistedCoordinatorJob cp = BuildPersistedJob(job, columnDefinition, indexBuildInfo, currentAttempts);
                            cp.StartOffset = offset;
                            await catalogs.PersistCoordinatorJobAsync(database, cp).ConfigureAwait(false);
                        };

                        await IndexBackfillAsync(database, job.TableName, indexBuildInfo, startOffset, checkpoint).ConfigureAwait(false);
                    }
                    else if (job.ElementKind == SchemaElementKind.ForeignKey)
                    {
                        await ValidateForeignKeyOrRemoveAsync(database, job).ConfigureAwait(false);
                    }
                }

                if (current == SchemaElementState.Absent && nextState == SchemaElementState.DeleteOnly)
                {
                    // First step of an add sequence: create the element in DeleteOnly state.
                    if (job.ElementKind == SchemaElementKind.Column)
                    {
                        if (columnDefinition is null)
                            throw new CamusDBException(
                                CamusDBErrorCodes.InvalidInput,
                                $"A ColumnInfo is required to add column '{job.ElementName}' to table '{job.TableName}' (current state is Absent)"
                            );

                        await catalogs.ReplicateAddColumnInStateAsync(
                            database, job.TableName, columnDefinition, SchemaElementState.DeleteOnly
                        ).ConfigureAwait(false);
                    }
                    else
                    {
                        if (indexBuildInfo is null)
                            throw new CamusDBException(
                                CamusDBErrorCodes.InvalidInput,
                                $"An IndexBuildInfo is required to add index '{job.ElementName}' to table '{job.TableName}' (current state is Absent)"
                            );

                        await catalogs.ReplicateAddIndexInStateAsync(
                            database, job.TableName, indexBuildInfo, SchemaElementState.DeleteOnly
                        ).ConfigureAwait(false);
                    }
                }
                else
                {
                    // Subsequent steps: move the existing element to the next adjacent state.
                    await catalogs.ReplicateElementStateAsync(
                        database, job.TableName, job.ElementName, nextState, job.ElementKind
                    ).ConfigureAwait(false);
                }

                current = nextState;

                if (OnStepCompleted is not null)
                    await OnStepCompleted(nextState, database.Schema.SchemaVersion).ConfigureAwait(false);
            }
        }
        finally
        {
            if (current == job.TargetState)
                await catalogs.DeleteCoordinatorJobAsync(database, job.TableId, job.ElementName)
                    .ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Waits until every live node acknowledged the schema version that holds the element in
    /// <c>WriteOnly</c>, before the backfill or the validation pass reads the table.
    ///
    /// <para><b>What the acknowledgement means here.</b> A node acknowledges a version only after the
    /// commits that were in flight on it, and that wrote the table without the new element, have a
    /// terminal outcome (<c>DatabaseDescriptor.PublishSchemaApplied</c>). Every later commit of such
    /// a write is refused by the write-shape fence. So once every live node acknowledged, each write
    /// planned without the element is either committed, and the read that follows sees it, or it can
    /// never commit. Without this wait a commit could land after the backfill read its key range,
    /// and leave a row with no index entry, or an orphan under a validated foreign key.</para>
    ///
    /// <para><b>Full convergence, not the quorum backstop.</b> The post-commit gate of the
    /// <c>WriteOnly</c> step may have returned on a majority. A node outside that majority can still
    /// hold such a commit, so a majority is not enough to read. The proposal of <c>Public</c> requires
    /// the same full convergence anyway (the two-version gate), so this adds no new way to stall: it
    /// only moves that wait in front of the read.</para>
    ///
    /// <para>On a timeout the element stays in <c>WriteOnly</c>, which is a safe resting state, and
    /// the job stays persisted for a resume or for the caller's compensation.</para>
    /// </summary>
    private static async Task RequireEarlierWritersSettledAsync(DatabaseDescriptor database, SchemaChangeJob job)
    {
        long version = database.Schema.SchemaVersion;

        bool settled = await database.Kahuna.WaitForSchemaAcksAsync(
            database.Id,
            version,
            database.Kahuna.SchemaAckWaitTimeout,
            enforceFullConvergence: true,
            cancellationToken: CancellationToken.None
        ).ConfigureAwait(false);

        if (settled)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Timed out waiting for every live node to settle the transactions that wrote table '{job.TableName}' of database " +
            $"'{database.Name}' before {job.ElementKind.ToString().ToLowerInvariant()} '{job.ElementName}' existed " +
            $"(schema version {version}); a node has not applied the change, or a commit that began before it has no outcome yet"
        );
    }

    /// <summary>
    /// Runs the foreign-key validation pass. On a violation, removes the constraint on every node and
    /// deletes the job before rethrowing: a constraint that failed validation must neither stay
    /// half-rolled-out nor be retried by a resume, which would fail the same way.
    /// </summary>
    private async Task ValidateForeignKeyOrRemoveAsync(DatabaseDescriptor database, SchemaChangeJob job)
    {
        if (ForeignKeyValidationAsync is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Foreign key '{job.ElementName}' on table '{job.TableName}' cannot be published: no validation pass is wired");

        try
        {
            await ForeignKeyValidationAsync(database, job.TableName, job.ElementName).ConfigureAwait(false);
        }
        catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.ForeignKeyViolation)
        {
            // Read before the removal: the same delta clears the index's owner, and afterwards nothing
            // tells the index the engine built apart from one the user made.
            string? ownedIndexName = FindOwnedIndexName(database.Schema, job.TableName, job.ElementName);

            await catalogs.ReplicateElementStateAsync(
                database, job.TableName, job.ElementName, SchemaElementState.Absent, SchemaElementKind.ForeignKey
            ).ConfigureAwait(false);

            if (ownedIndexName is not null)
                await DropReleasedIndexAsync(database, job.TableName, ownedIndexName).ConfigureAwait(false);

            await catalogs.DeleteCoordinatorJobAsync(database, job.TableId, job.ElementName).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Validates the existing rows against a CHECK or a NOT NULL constraint that is already enforced on
    /// every node, then deletes the job that <paramref name="job"/> names. The proposer of
    /// <c>ADD CONSTRAINT ... CHECK</c> and <c>SET NOT NULL</c> calls it after the replication, and a new
    /// leader calls it for a job it finds.
    ///
    /// <para><b>The order is the point.</b> It first waits until every live node settled the commits
    /// that wrote the table before the constraint existed (<see cref="RequireEarlierWritersSettledAsync"/>),
    /// so the read that follows sees each such row, and each later write was checked by its own
    /// statement. A read before the enforcement can miss a write that commits between the two.</para>
    ///
    /// <para>On a violation it removes the constraint on every node, deletes the job and rethrows: a
    /// resume would fail the same way. Any other failure, for example a lost leadership, leaves the job
    /// for the next leader, and the constraint stays enforced until that leader validates it.</para>
    /// </summary>
    public async Task ValidateRowConstraintAsync(DatabaseDescriptor database, SchemaChangeJob job)
    {
        if (RowConstraintValidationAsync is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Constraint '{job.ElementName}' on table '{job.TableName}' cannot be validated: no row validation pass is wired");

        await RequireEarlierWritersSettledAsync(database, job).ConfigureAwait(false);

        try
        {
            await RowConstraintValidationAsync(database, job.TableName, job.ElementKind, job.ElementName).ConfigureAwait(false);
        }
        catch (CamusDBException ex) when (ex.Code is CamusDBErrorCodes.CheckConstraintViolation or CamusDBErrorCodes.NotNullViolation)
        {
            await RemoveRowConstraintAsync(database, job).ConfigureAwait(false);
            await catalogs.DeleteCoordinatorJobAsync(database, job.TableId, job.ElementName).ConfigureAwait(false);
            throw;
        }

        await catalogs.DeleteCoordinatorJobAsync(database, job.TableId, job.ElementName).ConfigureAwait(false);
    }

    /// <summary>
    /// Takes back a CHECK or NOT NULL constraint that failed validation. A constraint that is already
    /// gone needs nothing. A NOT NULL job exists only for a column that was nullable before, so the
    /// column becomes nullable again, with no constraint name.
    /// </summary>
    private async Task RemoveRowConstraintAsync(DatabaseDescriptor database, SchemaChangeJob job)
    {
        if (!database.Schema.Tables.TryGetValue(job.TableName, out TableSchema? table))
            return;

        if (job.ElementKind == SchemaElementKind.Check)
        {
            if (table.CheckConstraints?.Exists(c => string.Equals(c.Name, job.ElementName, StringComparison.OrdinalIgnoreCase)) == true)
                await catalogs.ReplicateDropCheckConstraintAsync(database, job.TableName, job.ElementName).ConfigureAwait(false);

            return;
        }

        TableColumnSchema? column = table.Columns?.Find(
            c => c.NotNull && string.Equals(c.NotNullConstraintName, job.ElementName, StringComparison.OrdinalIgnoreCase));

        if (column is not null)
            await catalogs.ReplicateSetColumnNotNullAsync(database, job.TableName, column.Name, notNull: false, constraintName: null).ConfigureAwait(false);
    }

    /// <summary>
    /// True when <paramref name="table"/> still holds the CHECK or NOT NULL constraint that a job of
    /// <paramref name="kind"/> names. A resumed job whose constraint is gone has nothing to validate.
    /// </summary>
    private static bool RowConstraintExists(TableSchema table, SchemaElementKind kind, string constraintName) =>
        kind == SchemaElementKind.Check
            ? table.CheckConstraints?.Exists(c => string.Equals(c.Name, constraintName, StringComparison.OrdinalIgnoreCase)) == true
            : table.Columns?.Exists(c => c.NotNull && string.Equals(c.NotNullConstraintName, constraintName, StringComparison.OrdinalIgnoreCase)) == true;

    /// <summary>
    /// The name of the index that the foreign key <paramref name="constraintName"/> of
    /// <paramref name="tableName"/> owns, or null when the constraint reuses an index the user made.
    /// </summary>
    internal static string? FindOwnedIndexName(Schema schema, string tableName, string constraintName)
    {
        if (!schema.Tables.TryGetValue(tableName, out TableSchema? table))
            return null;

        ForeignKeySchema? foreignKey = table.ForeignKeys?.Find(fk => string.Equals(fk.Name, constraintName, StringComparison.OrdinalIgnoreCase));
        if (foreignKey is null)
            return null;

        return table.Indexes?.Find(ix => string.Equals(ix.OwnerConstraintId, foreignKey.Id, StringComparison.Ordinal))?.Name;
    }

    /// <summary>
    /// Best effort: the constraint is already gone, so a failure here must not hide the violation the
    /// caller is about to report. An index left behind is an ordinary index; it still serves queries.
    /// </summary>
    private async Task DropReleasedIndexAsync(DatabaseDescriptor database, string tableName, string indexName)
    {
        try
        {
            if (DropIndexAsync is not null)
                await DropIndexAsync(database, tableName, indexName).ConfigureAwait(false);
            else
                await catalogs.ReplicateDropIndexAsync(database, tableName, indexName).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger?.LogWarning(
                ex,
                "A foreign key on {TableName} failed validation and was removed, but its index {IndexName} could not be dropped",
                tableName,
                indexName);
        }
    }

    private static PersistedCoordinatorJob BuildPersistedJob(SchemaChangeJob job, ColumnInfo? columnDefinition, IndexBuildInfo? indexBuildInfo, int attempts) => new()
    {
        TableName = job.TableName,
        TableId = job.TableId,
        ElementName = job.ElementName,
        TargetState = job.TargetState,
        ElementKind = job.ElementKind,
        ColumnType = columnDefinition?.Type,
        ColumnNotNull = columnDefinition?.NotNull ?? false,
        ColumnDefault = columnDefinition?.Default,
        IndexId = indexBuildInfo?.IndexId,
        IndexColumnIds = indexBuildInfo?.ColumnIds,
        IndexType = indexBuildInfo?.IndexType,
        IndexIncludeColumnIds = indexBuildInfo?.IncludeColumnIds,
        IndexColumnDirections = indexBuildInfo?.ColumnDirections,
        Attempts = attempts,
    };

    /// <summary>
    /// Loads all persisted coordinator jobs for <paramref name="database"/> and
    /// drives each to its target state.  Called by the schema leader callback
    /// when this node wins a new election so interrupted sequences resume.
    /// </summary>
    public async Task ResumeJobsAsync(DatabaseDescriptor database)
    {
        // Retry with backoff: OnLeaderChanged fires before the KV state machine has applied all
        // committed Raft entries on the new leader. The coordinator job written by the previous
        // leader may not be visible on the first read. Retrying closes that window without
        // requiring a Kahuna API change for linearizable range scans.
        const int MaxAttempts = 10;
        List<PersistedCoordinatorJob> jobs = [];

        for (int attempt = 0; attempt < MaxAttempts; attempt++)
        {
            try
            {
                jobs = await catalogs.LoadCoordinatorJobsAsync(database).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "Failed to load coordinator jobs for database {DbName} on leader resume; skipping", database.Name);
                return;
            }

            if (jobs.Count > 0 || attempt == MaxAttempts - 1)
                break;

            await Task.Delay(100).ConfigureAwait(false);
        }

        foreach (PersistedCoordinatorJob persisted in jobs)
        {
            // Anchor resolution on the immutable table id. Find the live table whose schema
            // id matches. If no table matches, or the table named in the record now has a different
            // id (drop + recreate under the same name), the job is stale — delete it and skip.
            TableSchema? liveTable = database.Schema.Tables.Values
                .FirstOrDefault(t => t.Id == persisted.TableId);

            if (liveTable is null)
            {
                logger?.LogWarning(
                    "Deleting stale coordinator job {TableName}.{ElementName}: table id '{TableId}' no longer exists in the schema (table was dropped)",
                    persisted.TableName, persisted.ElementName, persisted.TableId);
                try { await catalogs.DeleteCoordinatorJobAsync(database, persisted.TableId, persisted.ElementName).ConfigureAwait(false); }
                catch (Exception ex) { logger?.LogWarning(ex, "Failed to delete stale coordinator job for database {DbName}", database.Name); }
                continue;
            }

            // Use the live table's current name for all schema operations (name-based API).
            string liveTableName = liveTable.Name ?? persisted.TableName;
            SchemaChangeJob job = new(database.Name, liveTableName, persisted.TableId, persisted.ElementName, persisted.TargetState, persisted.ElementKind);

            // Abandon a job that keeps failing across leader changes rather than retry it on
            // every election forever. A terminal failure (unreachable invariant, persistent
            // validation error) burns one attempt per resume; once the budget is spent we
            // delete + log loudly instead of looping.
            if (persisted.Attempts >= MaxResumeAttempts)
            {
                logger?.LogError(
                    "Abandoning coordinator job {TableName}.{ElementName} → {TargetState} on database {DbName} after {Attempts} resume attempts",
                    liveTableName, persisted.ElementName, persisted.TargetState, database.Name, persisted.Attempts);
                try { await catalogs.DeleteCoordinatorJobAsync(database, persisted.TableId, persisted.ElementName).ConfigureAwait(false); }
                catch (Exception ex) { logger?.LogWarning(ex, "Failed to delete abandoned coordinator job for database {DbName}", database.Name); }
                continue;
            }

            if (persisted.ElementKind is SchemaElementKind.Check or SchemaElementKind.NotNull)
            {
                await ResumeRowConstraintJobAsync(database, persisted, liveTable, job).ConfigureAwait(false);
                continue;
            }

            ColumnInfo? columnDefinition = persisted.ColumnType.HasValue
                ? new ColumnInfo(persisted.ElementName, persisted.ColumnType.Value, persisted.ColumnNotNull, persisted.ColumnDefault)
                : null;

            IndexBuildInfo? indexBuildInfo = null;
            if (persisted.ElementKind == SchemaElementKind.Index &&
                persisted.IndexId is not null &&
                persisted.IndexColumnIds is not null &&
                persisted.IndexType.HasValue)
            {
                string[] columnNames = ResolveColumnNames(liveTable, persisted.IndexColumnIds);
                string[]? includeColumnNames = persisted.IndexIncludeColumnIds is { Length: > 0 }
                    ? ResolveColumnNames(liveTable, persisted.IndexIncludeColumnIds)
                    : null;
                indexBuildInfo = new(persisted.IndexId, persisted.ElementName, persisted.IndexColumnIds, columnNames, persisted.IndexType.Value, ColumnDirections: persisted.IndexColumnDirections, IncludeColumnIds: persisted.IndexIncludeColumnIds, IncludeColumnNames: includeColumnNames);
            }

            try
            {
                SchemaElementState current = GetCurrentElementState(database.Schema, liveTableName, job.ElementName, job.ElementKind);
                SchemaElementState[] path = ComputePathFor(job.ElementKind, current, job.TargetState);

                if (path.Length == 0)
                {
                    // Already at target — e.g. the previous leader completed the last step but
                    // crashed before deleting the record. Clean it up rather than leave it.
                    await catalogs.DeleteCoordinatorJobAsync(database, persisted.TableId, job.ElementName).ConfigureAwait(false);
                    continue;
                }

                // Record this resume attempt durably BEFORE driving, so a crash mid-resume still
                // counts against the budget and the job can't be retried indefinitely.
                persisted.Attempts++;
                await catalogs.PersistCoordinatorJobAsync(database, persisted).ConfigureAwait(false);

                if (logger is not null)
                    Log.LogResumingCoordinatorJob(logger, liveTableName, persisted.ElementName, persisted.TargetState, database.Name, persisted.Attempts);

                await DriveToTargetAsync(database, job, columnDefinition, indexBuildInfo, persisted.StartOffset, current, path, persisted.Attempts, CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex,
                    "Coordinator resume failed for {TableName}.{ElementName} → {TargetState} on database {DbName}",
                    liveTableName, persisted.ElementName, persisted.TargetState, database.Name);
            }
        }
    }

    /// <summary>
    /// Finishes a CHECK or NOT NULL job that a previous leader left: the constraint is enforced, but its
    /// existing rows were not proven. A job whose constraint is gone is deleted. Otherwise the attempt
    /// is recorded first, as for every resumed job, and the rows are validated. A failure is logged,
    /// not thrown, so one job cannot stop the resume of the others.
    /// </summary>
    private async Task ResumeRowConstraintJobAsync(DatabaseDescriptor database, PersistedCoordinatorJob persisted, TableSchema liveTable, SchemaChangeJob job)
    {
        try
        {
            if (!RowConstraintExists(liveTable, persisted.ElementKind, persisted.ElementName))
            {
                await catalogs.DeleteCoordinatorJobAsync(database, persisted.TableId, persisted.ElementName).ConfigureAwait(false);
                return;
            }

            persisted.Attempts++;
            await catalogs.PersistCoordinatorJobAsync(database, persisted).ConfigureAwait(false);

            if (logger is not null)
                Log.LogResumingCoordinatorJob(logger, job.TableName, persisted.ElementName, persisted.TargetState, database.Name, persisted.Attempts);

            await ValidateRowConstraintAsync(database, job).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger?.LogError(ex,
                "Coordinator resume failed to validate {ElementKind} constraint {TableName}.{ElementName} on database {DbName}",
                persisted.ElementKind, job.TableName, persisted.ElementName, database.Name);
        }
    }

    /// <summary>
    /// Maximum number of leader-change resume attempts before a job is abandoned. A transient
    /// failure (leadership flap) normally completes within one or two resumes; exhausting this
    /// budget means the job is genuinely stuck, so it is deleted and logged rather than retried
    /// on every future election.
    /// </summary>
    private const int MaxResumeAttempts = 5;

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static SchemaElementState GetCurrentElementState(Schema schema, string tableName, string elementName, SchemaElementKind kind)
    {
        if (!schema.Tables.TryGetValue(tableName, out TableSchema? tableSchema))
            return SchemaElementState.Absent;

        if (kind == SchemaElementKind.Index)
        {
            TableIndexSchema? index = tableSchema.Indexes?.FirstOrDefault(ix => string.Equals(ix.Name, elementName, StringComparison.OrdinalIgnoreCase));
            return index?.State ?? SchemaElementState.Absent;
        }

        if (kind == SchemaElementKind.ForeignKey)
        {
            ForeignKeySchema? foreignKey = tableSchema.ForeignKeys?.FirstOrDefault(fk => string.Equals(fk.Name, elementName, StringComparison.OrdinalIgnoreCase));
            return foreignKey?.State ?? SchemaElementState.Absent;
        }

        TableColumnSchema? column = tableSchema.Columns?.FirstOrDefault(c => string.Equals(c.Name, elementName, StringComparison.OrdinalIgnoreCase));
        return column?.State ?? SchemaElementState.Absent;
    }

    private static string[] ResolveColumnNames(TableSchema table, string[] columnIds)
    {
        string[] names = new string[columnIds.Length];
        for (int i = 0; i < columnIds.Length; i++)
        {
            TableColumnSchema? col = table.Columns?.FirstOrDefault(c => c.Id == columnIds[i]);
            names[i] = col?.Name ?? columnIds[i];
        }
        return names;
    }

    /// <summary>
    /// The transition path for an element of <paramref name="kind"/>. A foreign key moves in one step
    /// (see <see cref="Apply.ElementStateApplier.ValidateForeignKeyStateTransition"/>), and a constraint
    /// that is already Absent has nothing left to drive: it was removed — by a failed validation, or
    /// by a DROP — and is never re-added by a job.
    /// </summary>
    internal static SchemaElementState[] ComputePathFor(SchemaElementKind kind, SchemaElementState from, SchemaElementState to)
    {
        if (kind != SchemaElementKind.ForeignKey)
            return ComputeTransitionPath(from, to);

        if (from == to || from == SchemaElementState.Absent)
            return [];

        return [to];
    }

    /// <summary>
    /// Returns the ordered sequence of states the element must pass through to
    /// reach <paramref name="to"/> from <paramref name="from"/>, excluding
    /// <paramref name="from"/> itself and including <paramref name="to"/>.
    /// Returns an empty array when already at the target.
    /// </summary>
    internal static SchemaElementState[] ComputeTransitionPath(SchemaElementState from, SchemaElementState to)
    {
        if (from == to)
            return [];

        int fromIdx = Array.IndexOf(StateOrder, from);
        int toIdx = Array.IndexOf(StateOrder, to);

        if (fromIdx < toIdx)
        {
            // Forward direction: Absent → DeleteOnly → WriteOnly → Public
            return StateOrder[(fromIdx + 1)..(toIdx + 1)];
        }
        else
        {
            // Reverse direction: Public → WriteOnly → DeleteOnly → Absent
            SchemaElementState[] path = new SchemaElementState[fromIdx - toIdx];
            for (int i = 0; i < path.Length; i++)
                path[i] = StateOrder[fromIdx - 1 - i];
            return path;
        }
    }
}

/// <summary>
/// Describes a single online-schema-change target.
/// </summary>
/// <param name="DatabaseName">The database the table belongs to.</param>
/// <param name="TableName">The table whose element is being transitioned.</param>
/// <param name="ElementName">The column or index name.</param>
/// <param name="TargetState">The desired final state for the element.</param>
/// <param name="ElementKind">Whether the element is a column (default) or an index.</param>
public sealed record SchemaChangeJob(
    string DatabaseName,
    string TableName,
    string TableId,
    string ElementName,
    SchemaElementState TargetState,
    SchemaElementKind ElementKind = SchemaElementKind.Column
);

/// <summary>
/// Carries the immutable metadata needed to (re)build an index during coordinator-driven
/// backfill. Passed to <see cref="SchemaChangeCoordinator.IndexBackfillAsync"/> on both
/// the initial run and any leader-change resume.
/// </summary>
/// <param name="IndexId">Immutable index ID (used to locate the schema entry).</param>
/// <param name="IndexName">Index name (KV key prefix for <c>PutIndexEntry</c>).</param>
/// <param name="ColumnIds">Immutable column IDs covered by the index.</param>
/// <param name="ColumnNames">Resolved column names, populated from the table schema.</param>
/// <param name="IndexType">Whether the index enforces uniqueness.</param>
/// <param name="ColumnDirections">
/// Per-column sort direction, positionally aligned with <paramref name="ColumnIds"/>; null means
/// all-ascending. Carried through the staged cluster add so the replicated index definition
/// records the same directions the proposer parsed.
/// </param>
public sealed record IndexBuildInfo(
    string IndexId,
    string IndexName,
    string[] ColumnIds,
    string[] ColumnNames,
    IndexType IndexType,
    OrderType[]? ColumnDirections = null,
    string[]? IncludeColumnIds = null,
    string[]? IncludeColumnNames = null
);
