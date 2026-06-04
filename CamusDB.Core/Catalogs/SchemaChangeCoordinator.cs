
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
/// This implements the <em>two-version invariant</em> (architecture doc §6.2) for
/// multi-step sequences: after each step all nodes are on the same version before the
/// coordinator advances to the next, so the cluster never has more than two adjacent
/// schema versions in flight simultaneously.
/// </para>
///
/// <para>
/// The coordinator is stateless beyond the job description; persistence for leader-change
/// resume is added by D2.  It must be called on the schema leader — followers should
/// forward DDL via the production HTTP path (Workstream C) rather than running the
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
    /// <para>
    /// Requires <c>!database.OwnsKahuna</c>: the coordinator only makes sense in
    /// a cluster where schema changes must be replicated.
    /// </para>
    /// </summary>
    public async Task RunJobAsync(
        DatabaseDescriptor database,
        SchemaChangeJob job,
        ColumnInfo? columnDefinition = null,
        CancellationToken cancellationToken = default
    )
    {
        if (database.OwnsKahuna)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "SchemaChangeCoordinator requires a cluster database (OwnsKahuna must be false)"
            );

        SchemaElementState current = GetCurrentColumnState(database.Schema, job.TableName, job.ElementName);
        SchemaElementState[] path = ComputeTransitionPath(current, job.TargetState);

        if (path.Length == 0)
            return;

        // Persist the job so a new leader can resume if this coordinator is interrupted.
        await catalogs.PersistCoordinatorJobAsync(database, new PersistedCoordinatorJob
        {
            TableName = job.TableName,
            ElementName = job.ElementName,
            TargetState = job.TargetState,
            ColumnType = columnDefinition?.Type,
            ColumnNotNull = columnDefinition?.NotNull ?? false,
            ColumnDefault = columnDefinition?.Default,
        }).ConfigureAwait(false);

        try
        {
            foreach (SchemaElementState nextState in path)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (current == SchemaElementState.Absent && nextState == SchemaElementState.DeleteOnly)
                {
                    // First step of an add sequence: create the column in DeleteOnly state.
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
                    // Subsequent steps: move the existing column to the next adjacent state.
                    await catalogs.ReplicateElementStateAsync(
                        database, job.TableName, job.ElementName, nextState
                    ).ConfigureAwait(false);
                }

                current = nextState;

                if (OnStepCompleted is not null)
                    await OnStepCompleted(nextState, database.Schema.SchemaVersion).ConfigureAwait(false);
            }
        }
        finally
        {
            // Remove the durable job record on success OR on terminal failure so we don't
            // leave stale entries that would be re-run indefinitely by future leaders.
            // Transient failures (leadership loss) are expected to cause an exception here;
            // the new leader will pick up the persisted job and resume.
            if (current == job.TargetState)
            {
                await catalogs.DeleteCoordinatorJobAsync(database, job.TableName, job.ElementName)
                    .ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Loads all persisted coordinator jobs for <paramref name="database"/> and
    /// drives each to its target state.  Called by the schema leader callback
    /// (D2) when this node wins a new election so interrupted sequences resume.
    /// </summary>
    public async Task ResumeJobsAsync(DatabaseDescriptor database)
    {
        List<PersistedCoordinatorJob> jobs;
        try
        {
            jobs = await catalogs.LoadCoordinatorJobsAsync(database).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger?.LogWarning(ex, "Failed to load coordinator jobs for database {DbName} on leader resume; skipping", database.Name);
            return;
        }

        foreach (PersistedCoordinatorJob persisted in jobs)
        {
            ColumnInfo? columnDefinition = persisted.ColumnType.HasValue
                ? new ColumnInfo(persisted.ElementName, persisted.ColumnType.Value, persisted.ColumnNotNull, persisted.ColumnDefault)
                : null;

            try
            {
                logger?.LogInformation(
                    "Resuming coordinator job for {TableName}.{ElementName} → {TargetState} on database {DbName}",
                    persisted.TableName, persisted.ElementName, persisted.TargetState, database.Name);

                await RunJobAsync(
                    database,
                    new SchemaChangeJob(database.Name, persisted.TableName, persisted.ElementName, persisted.TargetState),
                    columnDefinition
                ).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex,
                    "Coordinator resume failed for {TableName}.{ElementName} → {TargetState} on database {DbName}",
                    persisted.TableName, persisted.ElementName, persisted.TargetState, database.Name);
            }
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static SchemaElementState GetCurrentColumnState(Schema schema, string tableName, string elementName)
    {
        if (!schema.Tables.TryGetValue(tableName, out TableSchema? tableSchema))
            return SchemaElementState.Absent;

        TableColumnSchema? column = tableSchema.Columns?.FirstOrDefault(c => c.Name == elementName);
        return column?.State ?? SchemaElementState.Absent;
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
/// <param name="ElementName">The column (or future index) name.</param>
/// <param name="TargetState">The desired final state for the element.</param>
public sealed record SchemaChangeJob(
    string DatabaseName,
    string TableName,
    string ElementName,
    SchemaElementState TargetState
);
