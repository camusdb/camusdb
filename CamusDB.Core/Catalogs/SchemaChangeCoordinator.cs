
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
        SchemaElementState[] path = ComputeTransitionPath(current, job.TargetState);

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
                indexBuildInfo = new(persisted.IndexId, persisted.ElementName, persisted.IndexColumnIds, columnNames, persisted.IndexType.Value);
            }

            try
            {
                SchemaElementState current = GetCurrentElementState(database.Schema, liveTableName, job.ElementName, job.ElementKind);
                SchemaElementState[] path = ComputeTransitionPath(current, job.TargetState);

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
            TableIndexSchema? index = tableSchema.Indexes?.FirstOrDefault(ix => ix.Name == elementName);
            return index?.State ?? SchemaElementState.Absent;
        }

        TableColumnSchema? column = tableSchema.Columns?.FirstOrDefault(c => c.Name == elementName);
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
    OrderType[]? ColumnDirections = null
);
