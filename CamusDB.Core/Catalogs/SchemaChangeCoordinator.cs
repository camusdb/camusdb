
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
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
    /// One resume at a time per database on this coordinator. The leader callback, the open-time
    /// resume, the sweep and a statement waiting for a handed-over job can all ask for a resume at
    /// once; two resumes running together would each pick the same job up, bump its attempts twice
    /// and run two backfills over the same rows. A request that finds a resume running marks it as
    /// pending, and the running resume goes round again before it ends, so no request is lost.
    /// </summary>
    private readonly ConcurrentDictionary<string, ResumeRun> resumeRuns = new();

    /// <summary>
    /// When a job that failed its last attempt on this node may be driven again, by job key
    /// (<see cref="RetryKey"/>). The sweep would otherwise retry a failing job at every tick and burn
    /// its attempt budget in a minute; a failed attempt waits <see cref="ResumeRetryBackoff"/>, doubled
    /// per attempt up to <see cref="ResumeRetryBackoffMax"/>. Node-local: another node that takes
    /// the job over starts afresh, as it should.
    /// </summary>
    private readonly ConcurrentDictionary<string, DateTime> retryNotBefore = new();

    /// <summary>The wait after the first failed attempt of a job before it is driven again on this node.</summary>
    public TimeSpan ResumeRetryBackoff { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>The longest wait between two attempts of a job on this node.</summary>
    public TimeSpan ResumeRetryBackoffMax { get; set; } = TimeSpan.FromMinutes(2);

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
    /// <c>DatabaseOpener</c>. The token is cancelled when this node loses the schema leadership
    /// while the fill runs (<see cref="EmbeddedKahuna.RegisterSchemaLeadershipLostCallback"/>): the
    /// job is the next leader's from then on, and the fill must stop at its next batch.
    /// </summary>
    public Func<DatabaseDescriptor, string, ColumnInfo, CancellationToken, Task>? BackfillAsync { get; set; }

    /// <summary>
    /// Optional delegate invoked once, just before an index transitions from
    /// <c>WriteOnly</c> to <c>Public</c>. Receives the database, table name, index build
    /// info, the last committed backfill offset (null = start from beginning), and a
    /// checkpoint callback the implementation must invoke after each committed batch with
    /// the last processed rowId so a leader-change resume can skip already-indexed rows.
    /// Idempotent: using <c>backfillMode: true</c> in <c>PutIndexEntry</c> ensures re-runs
    /// on resume are safe even for unique indexes. The token is cancelled when this node loses the
    /// schema leadership while the backfill runs, as for <see cref="BackfillAsync"/>.
    /// </summary>
    public Func<DatabaseDescriptor, string, IndexBuildInfo, string?, Func<string, Task>?, CancellationToken, Task>? IndexBackfillAsync { get; set; }

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

    /// <summary>
    /// Delegate that drops the sequence an added identity column owned, after that column was removed
    /// again. Receives the database, the table id and the sequence id. It must drop the sequence only
    /// when the table owns it and no column still draws from it. Set on the command-path coordinator and
    /// on the resume coordinator in <c>DatabaseOpener</c>; without it the sequence stays, owned by the
    /// table, and goes when the table goes.
    /// </summary>
    public Func<DatabaseDescriptor, string, string, Task>? ReleaseOwnedSequenceAsync { get; set; }

    /// <summary>
    /// The schema-leader term the last job run or resume on this coordinator read when it started
    /// (<see cref="EmbeddedKahuna.SchemaLeaderTerm"/>). Every job-record write of that run carries it,
    /// so a statement's compensation can fence its own job-record delete with the same term.
    /// </summary>
    public long LeaderTerm { get; private set; }

    /// <summary>
    /// Test-only seam: replaces the read of the schema-leader term, so a test on one node can play a
    /// successor that took a job over in a newer term. Null in production.
    /// </summary>
    internal Func<DatabaseDescriptor, long>? ReadLeaderTermForTesting;

    /// <summary>
    /// How long a driver that handed its job over waits for the next schema leader's outcome before
    /// it answers that the change is still in progress. The successor fills the same table the driver
    /// was filling, so this is a budget for a whole fill, not for a round trip. Settable by tests.
    /// </summary>
    internal static TimeSpan HandoverOutcomeWait = TimeSpan.FromMinutes(10);

    /// <summary>The interval at which a driver that handed its job over reads the job record.</summary>
    internal static TimeSpan HandoverOutcomePoll = TimeSpan.FromMilliseconds(250);

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
    /// <para><b>The DDL gate.</b> Each delta the job proposes is proposed under the database's
    /// <see cref="SchemaDdlGate"/>, and the job holds its table for its whole run
    /// (<see cref="SchemaDdlGate.HoldTable"/>). Between the <c>WriteOnly</c> step and the
    /// <c>Public</c> step, while the job waits for the earlier writers to settle and then backfills or
    /// validates every row of the table, the gate is <em>released</em>: that phase proposes nothing,
    /// and it is the one that takes minutes on a loaded table. DDL on other tables of the database runs
    /// meanwhile; DDL on this table waits for the hold. <paramref name="gateHeldByCaller"/> says
    /// whether the caller entered the gate before handing the job over, as the DDL statements do, so
    /// that it finds the gate held again when this method returns or throws, and its own compensation
    /// and its <c>finally</c> release are balanced. A caller that holds nothing, such as the resume on
    /// a new leader, passes false and gets the gate back released.</para>
    ///
    /// <para><b>A job whose driver lost the schema leadership is answered from its outcome.</b> The
    /// job record is stamped with the schema-leader term this node read when it took the job on, and
    /// no record write of an older term lands over it (<c>CoordinatorJobStore</c>); the fill is
    /// cancelled the moment the leadership is lost. A failure that follows a leadership change is not
    /// this driver's to repair or to report as "nothing was committed": the record stays, the next
    /// leader's resume drives it, and this method waits for that outcome
    /// (<see cref="AnswerFromHandedOverJobAsync"/>). It returns normally when the successor brought
    /// the element to the target, and throws <see cref="SchemaChangeHandedOverException"/> when the
    /// successor took it back, with the reason it recorded, or when the job is still in progress after
    /// <see cref="HandoverOutcomeWait"/>. When this node leads again meanwhile, it drives the job to
    /// its end itself, through the resume.</para>
    /// </summary>
    public async Task RunJobAsync(
        DatabaseDescriptor database,
        SchemaChangeJob job,
        ColumnInfo? columnDefinition = null,
        IndexBuildInfo? indexBuildInfo = null,
        CancellationToken cancellationToken = default,
        bool gateHeldByCaller = false
    )
    {
        SchemaDdlGateHold gate = new(database.DdlGate, gateHeldByCaller);
        long term = ReadLeaderTerm(database);
        LeaderTerm = term;

        try
        {
            // The state is read, and the job recorded, under the gate: a drop or a rename of the table
            // cannot slip between the read and the first delta, and the record is visible to a branch
            // create that checks for pending jobs under the same gate.
            await gate.AcquireAsync().ConfigureAwait(false);

            SchemaElementState current = GetCurrentElementState(database.Schema, job.TableName, job.ElementName, job.ElementKind);
            SchemaElementState[] path = ComputePathFor(job.ElementKind, current, job.TargetState);

            if (path.Length == 0)
                return;

            // Persist the job (attempt 0) so a new leader can resume if this coordinator is
            // interrupted. ResumeJobsAsync bumps the attempt count on each pickup and abandons the
            // job once it exhausts the retry budget, so a doomed job can't loop forever.
            await catalogs.PersistCoordinatorJobAsync(
                database, BuildPersistedJob(job, columnDefinition, indexBuildInfo, attempts: 0, term)
            ).ConfigureAwait(false);

            await DriveToTargetAsync(database, job, columnDefinition, indexBuildInfo, startOffset: null, current, path, currentAttempts: 0, term, gate, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            if (!JobWasHandedOver(database, term, ex))
                throw;

            await AnswerFromHandedOverJobAsync(database, job, term, ex, gate).ConfigureAwait(false);
        }
        finally
        {
            await gate.RestoreAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The schema-leader term this node reads now, or zero when it is not known (a node that does not
    /// host the schema log). A zero term fences nothing.
    /// </summary>
    private long ReadLeaderTerm(DatabaseDescriptor database) =>
        ReadLeaderTermForTesting is { } read ? read(database) : database.Kahuna.SchemaLeaderTerm(database.Id);

    /// <summary>
    /// Whether a failure of a job this node drove under <paramref name="term"/> means the job is no
    /// longer this node's: a record write was refused because a newer term owns the record, or the
    /// schema-leader term moved since the job started. The second covers a step the schema log
    /// refused as not-leader, a fill cancelled by the loss of leadership, and any other failure that
    /// came with a leadership change: whatever the failure was, the record is now driven by the
    /// current leader, and this driver must neither take the element back nor report a definite
    /// failure on its own. A node that reads no term (zero) is never fenced.
    /// </summary>
    private bool JobWasHandedOver(DatabaseDescriptor database, long term, Exception failure) =>
        failure is CoordinatorJobTakenOverException
        || (term > 0 && ReadLeaderTerm(database) != term);

    /// <summary>
    /// Waits for the outcome of a job this node handed over to the next schema leader, and answers
    /// the statement that started it from that outcome. The gate is released while this waits, and
    /// held again on return. The table hold of the job is already gone, so a resume on this node can
    /// drive the job when this node leads again; this method runs that resume itself when it sees
    /// that the record is older than the current term and nobody holds the table, which is the case
    /// where this node was re-elected after its own resume had found the job still running.
    /// </summary>
    private async Task AnswerFromHandedOverJobAsync(DatabaseDescriptor database, SchemaChangeJob job, long term, Exception failure, SchemaDdlGateHold gate)
    {
        if (logger is not null)
            Log.LogSchemaChangeHandedOver(logger, DescribeElement(job), job.TableName, database.Name, term, failure.Message);

        gate.Release();

        try
        {
            DateTime deadline = DateTime.UtcNow.Add(HandoverOutcomeWait);
            PersistedCoordinatorJob? lastSeen = null;

            while (true)
            {
                PersistedCoordinatorJob? record = await catalogs.TryLoadCoordinatorJobAsync(database, job.TableId, job.ElementName).ConfigureAwait(false);

                if (record is null)
                    break;

                lastSeen = record;

                // A resume that fails leaves the record as it was; the next round drives it again
                // after the poll interval, not at once.
                if (await ThisNodeShouldDriveAsync(database, job, record).ConfigureAwait(false))
                    await ResumeJobsAsync(database, CoordinatorResumeTrigger.HandoverWait).ConfigureAwait(false);

                if (DateTime.UtcNow >= deadline)
                    throw new SchemaChangeHandedOverException(
                        CamusDBErrorCodes.InvalidInternalOperation,
                        $"The change to {DescribeElement(job)} of table '{job.TableName}' was handed over to the schema leader that followed " +
                        $"this node and is still in progress after {HandoverOutcomeWait.TotalSeconds:0} s; it completes in the background, " +
                        "check the table's schema before running the statement again");

                await Task.Delay(HandoverOutcomePoll).ConfigureAwait(false);
            }

            // The record is gone: the job finished. The local schema can trail its last delta by an
            // acknowledgement, so the state is read once it is settled.
            SchemaElementState state = await WaitForSettledStateAsync(database, job).ConfigureAwait(false);

            if (state == job.TargetState)
            {
                if (logger is not null)
                    Log.LogSchemaChangeCompletedBySuccessor(logger, DescribeElement(job), job.TableName, database.Name);
                return;
            }

            if (lastSeen is { FailureCode: { } code, FailureMessage: { } message })
                throw new SchemaChangeHandedOverException(
                    code,
                    $"{message} (the change to {DescribeElement(job)} of table '{job.TableName}' was handed over to the schema leader " +
                    "that followed this node, which took it back)");

            throw new SchemaChangeHandedOverException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"The change to {DescribeElement(job)} of table '{job.TableName}' was handed over to the schema leader that followed " +
                $"this node, which did not bring it to {job.TargetState} (the element is {state}); see that node's log for the reason");
        }
        finally
        {
            await gate.AcquireAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// True when the handed-over job has no live driver and this node is the one to drive it: this
    /// node leads the schema log again, in a term newer than the record's, and no staged change holds
    /// the table here. A record stamped with the current term is being driven by this node's own
    /// resume already, and one stamped with a newer term than this node knows belongs to a leader this
    /// node has not heard from yet.
    /// </summary>
    private async ValueTask<bool> ThisNodeShouldDriveAsync(DatabaseDescriptor database, SchemaChangeJob job, PersistedCoordinatorJob record)
    {
        long term = ReadLeaderTerm(database);

        if (term <= 0 || record.LeaderTerm >= term || database.DdlGate.IsTableHeld(job.TableName))
            return false;

        return await database.Kahuna.AmISchemaLeaderAsync(database.Id, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// The element's state once it is settled: its target or <c>Absent</c>, which are the two ends of
    /// a job. Waits up to the schema acknowledgement timeout for a last delta this node has not applied
    /// yet, and returns whatever state it reads then.
    /// </summary>
    private static async Task<SchemaElementState> WaitForSettledStateAsync(DatabaseDescriptor database, SchemaChangeJob job)
    {
        DateTime deadline = DateTime.UtcNow.Add(database.Kahuna.SchemaAckWaitTimeout);

        while (true)
        {
            SchemaElementState state = GetCurrentElementState(database.Schema, job.TableName, job.ElementName, job.ElementKind);

            if (state == job.TargetState || state == SchemaElementState.Absent || DateTime.UtcNow >= deadline)
                return state;

            await Task.Delay(HandoverOutcomePoll).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Runs the adjacent-transition steps in <paramref name="path"/>, gating on the cluster-wide
    /// ack of each version (enforced inside the <c>Replicate*</c> methods). Deletes the durable
    /// job record only when the element actually reaches the target; a transient failure (e.g.
    /// leadership loss) leaves the record so a new leader can resume it.
    ///
    /// <para>The caller holds <paramref name="gate"/>. Every delta is proposed with it held; the table
    /// is held for the whole run; and the long phase before <c>Public</c> runs with the gate released,
    /// as <see cref="RunJobAsync"/> describes. The gate is held again when this returns or throws.</para>
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
        long term,
        SchemaDdlGateHold gate,
        CancellationToken cancellationToken
    )
    {
        await gate.AcquireAsync().ConfigureAwait(false);

        using SchemaDdlGate.StagedTableHold tableHold = database.DdlGate.HoldTable(job.TableId, job.TableName, DescribeElement(job));

        // The fill or the backfill stops at its next batch when this node loses the schema
        // leadership: from then on the job is the next leader's, and two drivers filling one table
        // would fight over its row locks. A term this node does not know (zero) watches nothing.
        using CancellationTokenSource stopOnLeadershipLoss = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using IDisposable? leadershipWatch = term > 0
            ? database.Kahuna.RegisterSchemaLeadershipLostCallback(database.Id, lostTerm =>
            {
                if (lostTerm >= term)
                    stopOnLeadershipLoss.Cancel();

                return Task.CompletedTask;
            })
            : null;

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
                    // Nothing in this phase proposes a delta, and it reads or writes every row of the
                    // table, so the gate is released for it: DDL on the other tables of the database
                    // runs meanwhile, while the table hold keeps DDL on this table waiting. A
                    // compensation inside the phase takes the gate again before it proposes.
                    gate.Release();

                    if (logger is not null)
                        Log.LogStagedChangeReleasesDdlGate(logger, DescribeElement(job), job.TableName, PhaseName(job.ElementKind), database.Name);

                    try
                    {
                        if (job.ElementKind == SchemaElementKind.Column)
                        {
                            await SettleAndFillColumnOrTakeBackAsync(database, job, columnDefinition, currentAttempts, term, gate, stopOnLeadershipLoss.Token).ConfigureAwait(false);
                        }
                        else
                        {
                            await RequireEarlierWritersSettledAsync(database, job).ConfigureAwait(false);
                        }

                        if (job.ElementKind == SchemaElementKind.Index &&
                            IndexBackfillAsync is not null &&
                            indexBuildInfo is not null)
                        {
                            // Build a checkpoint callback: after each committed batch, persist the
                            // last processed rowId so a leader-change resume skips already-indexed rows.
                            // The write carries this driver's term: a checkpoint of a driver whose job
                            // the next leader took over is refused, and the backfill stops there.
                            Func<string, Task> checkpoint = async offset =>
                            {
                                PersistedCoordinatorJob cp = BuildPersistedJob(job, columnDefinition, indexBuildInfo, currentAttempts, term);
                                cp.StartOffset = offset;
                                await catalogs.PersistCoordinatorJobAsync(database, cp).ConfigureAwait(false);
                            };

                            await IndexBackfillAsync(database, job.TableName, indexBuildInfo, startOffset, checkpoint, stopOnLeadershipLoss.Token).ConfigureAwait(false);
                        }
                        else if (job.ElementKind == SchemaElementKind.ForeignKey)
                        {
                            await ValidateForeignKeyOrRemoveAsync(database, job, gate).ConfigureAwait(false);
                        }
                    }
                    finally
                    {
                        // Taken again whatever happened: the Public step, and the caller's compensation
                        // after a failure, both propose deltas.
                        await gate.AcquireAsync().ConfigureAwait(false);
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
            await gate.AcquireAsync().ConfigureAwait(false);

            if (current == job.TargetState)
            {
                try
                {
                    await catalogs.DeleteCoordinatorJobAsync(database, job.TableId, job.ElementName, term).ConfigureAwait(false);
                }
                catch (CoordinatorJobTakenOverException ex)
                {
                    // The element reached its target under this driver's last proposal, and the next
                    // leader took the record over meanwhile. Its resume finds nothing left to drive
                    // and deletes the record; this driver's statement is answered from the target.
                    logger?.LogInformation(ex.Message + "; the element is at its target and that leader deletes the record");
                }
            }
        }
    }

    /// <summary>The element a job changes, for the gate's hold and its log lines.</summary>
    private static string DescribeElement(SchemaChangeJob job) =>
        $"{job.ElementKind.ToString().ToLowerInvariant()} '{job.ElementName}'";

    /// <summary>What the long phase before <c>Public</c> does for an element kind, for log lines.</summary>
    private static string PhaseName(SchemaElementKind kind) => kind switch
    {
        SchemaElementKind.Column => "fill",
        SchemaElementKind.Index => "backfill",
        _ => "validation",
    };

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
    /// Waits for the earlier writers, then fills the existing rows of a column that is in
    /// <c>WriteOnly</c>. On any failure, takes the column back (<see cref="TakeBackColumnAsync"/>) and
    /// rethrows.
    ///
    /// <para><b>Why any failure, and not only a violation.</b> A column left in <c>WriteOnly</c> is
    /// invisible to queries, but its name is taken, so the same ADD COLUMN cannot run again, and a
    /// resume would only retry it after a leader change. A NOT NULL column over a row that would hold
    /// NULL fails on every retry. A fill that failed for a transient reason (a timeout, a lost
    /// leadership) can simply run again once the column is gone.</para>
    /// </summary>
    /// <para><b>Not after a leadership change.</b> A fill that failed because this node lost the
    /// schema leadership (its cancellation, a refused step, or any failure that came with a change of
    /// the term) is not taken back: the job is the current leader's, whose resume fills the column or
    /// takes it back itself, and this driver's statement waits for that outcome
    /// (<see cref="RunJobAsync"/>). A take-back that a newer term refuses is reported as the takeover
    /// it is, in place of the fill's own failure.</para>
    /// </summary>
    private async Task SettleAndFillColumnOrTakeBackAsync(
        DatabaseDescriptor database, SchemaChangeJob job, ColumnInfo? columnDefinition, int attempts, long term, SchemaDdlGateHold gate, CancellationToken cancellationToken)
    {
        try
        {
            await RequireEarlierWritersSettledAsync(database, job).ConfigureAwait(false);

            if (BackfillAsync is not null && columnDefinition is not null)
                await BackfillAsync(database, job.TableName, columnDefinition, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            if (JobWasHandedOver(database, term, ex))
            {
                if (logger is not null)
                    Log.LogFillStoppedByLeadershipLoss(logger, DescribeElement(job), job.TableName, database.Name, term);
                throw;
            }

            await TakeBackColumnAsync(database, job, columnDefinition, attempts, term, ex, gate).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Removes a column whose existing rows were not filled. Never throws: the caller is already
    /// failing, and its own error is the one to report.
    ///
    /// <para><b>The intent is durable before the removal starts.</b> The job is written again with the
    /// target <c>Absent</c>. A leader that finds the job after a crash, a lost leadership or a failed
    /// job delete then finishes the removal (<see cref="RemoveColumnAsync"/>). With the original target
    /// <c>Public</c> still recorded, that leader would add the column again, which no statement asked
    /// for. When the intent cannot be recorded, nothing is removed: the column stays in a state no query
    /// reads, under a job that still describes it.</para>
    ///
    /// <para>The intent records <paramref name="cause"/>, the error that failed the fill, so that a
    /// driver whose job this node took over can answer its client with that reason. A removal that a
    /// newer term refuses throws <see cref="CoordinatorJobTakenOverException"/>: the job is that
    /// leader's, and the caller reports the takeover rather than the fill's failure.</para>
    /// </summary>
    internal async Task TakeBackColumnAsync(DatabaseDescriptor database, SchemaChangeJob job, ColumnInfo? columnDefinition, int attempts, long term, Exception cause, SchemaDdlGateHold gate)
    {
        SchemaChangeJob removal = job with { TargetState = SchemaElementState.Absent };

        // The removal proposes deltas, so it runs under the gate, which the fill released.
        await gate.AcquireAsync().ConfigureAwait(false);

        try
        {
            PersistedCoordinatorJob intent = BuildPersistedJob(removal, columnDefinition, indexBuildInfo: null, attempts, term);
            intent.FailureCode = (cause as CamusDBException)?.Code ?? CamusDBErrorCodes.InvalidInternalOperation;
            intent.FailureMessage = cause.Message;

            await catalogs.PersistCoordinatorJobAsync(database, intent).ConfigureAwait(false);
        }
        catch (CoordinatorJobTakenOverException ex)
        {
            logger?.LogInformation(ex.Message + "; the unfilled column {ElementName} of {TableName} is that leader's to fill or to remove",
                job.ElementName, job.TableName);
            throw;
        }
        catch (Exception ex)
        {
            logger?.LogError(ex,
                "Could not record the removal of the unfilled column {ElementName} of {TableName}; it stays unreadable " +
                "under its add job for the next schema leader",
                job.ElementName, job.TableName);
            return;
        }

        await RemoveColumnAsync(database, removal, columnDefinition?.DefaultSequenceId, term).ConfigureAwait(false);
    }

    /// <summary>
    /// Drives a column down its staged states (<c>WriteOnly → DeleteOnly → Absent</c>), releases the
    /// sequence an identity column owned, and deletes the job, whose target is already <c>Absent</c>.
    /// Never throws. The job is deleted only when the column is gone; until then it is what lets the
    /// next leader finish the removal. Safe to run again at every step. The caller holds the DDL gate:
    /// every step here proposes a delta. The job delete carries <paramref name="term"/>, the remover's
    /// schema-leader term, and is refused when a newer term owns the record.
    /// </summary>
    private async Task RemoveColumnAsync(DatabaseDescriptor database, SchemaChangeJob job, string? defaultSequenceId, long term)
    {
        // A published column is never taken down by a removal job. The failed add never published it,
        // so a Public column of that name belongs to a later statement, and the job is stale.
        if (GetCurrentElementState(database.Schema, job.TableName, job.ElementName, SchemaElementKind.Column) == SchemaElementState.Public)
        {
            logger?.LogWarning(
                "Column {ElementName} of {TableName} is published; its stale removal job is deleted and the column stays",
                job.ElementName, job.TableName);

            try { await catalogs.DeleteCoordinatorJobAsync(database, job.TableId, job.ElementName, term).ConfigureAwait(false); }
            catch (Exception ex) { logger?.LogWarning(ex, "Could not delete the stale removal job of column {ElementName} of {TableName}", job.ElementName, job.TableName); }

            return;
        }

        try
        {
            SchemaElementState state = GetCurrentElementState(database.Schema, job.TableName, job.ElementName, SchemaElementKind.Column);

            if (state == SchemaElementState.WriteOnly)
            {
                await catalogs.ReplicateElementStateAsync(
                    database, job.TableName, job.ElementName, SchemaElementState.DeleteOnly, SchemaElementKind.Column
                ).ConfigureAwait(false);

                state = SchemaElementState.DeleteOnly;
            }

            if (state == SchemaElementState.DeleteOnly)
                await catalogs.ReplicateElementStateAsync(
                    database, job.TableName, job.ElementName, SchemaElementState.Absent, SchemaElementKind.Column
                ).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger?.LogWarning(ex,
                "Could not remove the unfilled column {ElementName} of {TableName}",
                job.ElementName, job.TableName);
        }

        if (GetCurrentElementState(database.Schema, job.TableName, job.ElementName, SchemaElementKind.Column) != SchemaElementState.Absent)
        {
            logger?.LogError(
                "The unfilled column {ElementName} of {TableName} is still in the schema; its removal job stays for the next schema leader",
                job.ElementName, job.TableName);
            return;
        }

        if (defaultSequenceId is not null && ReleaseOwnedSequenceAsync is not null)
        {
            try
            {
                await ReleaseOwnedSequenceAsync(database, job.TableId, defaultSequenceId).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // The job stays, so the next leader releases it. A sequence left behind is owned by the
                // table and goes when the table goes.
                logger?.LogWarning(ex,
                    "Could not drop the sequence of the removed column {ElementName} of {TableName}",
                    job.ElementName, job.TableName);
                return;
            }
        }

        try
        {
            await catalogs.DeleteCoordinatorJobAsync(database, job.TableId, job.ElementName, term).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // The job targets Absent, so the next leader finds the column gone and only deletes it.
            logger?.LogWarning(ex,
                "Could not delete the removal job of column {ElementName} of {TableName}; the next schema leader deletes it",
                job.ElementName, job.TableName);
        }
    }

    /// <summary>
    /// Runs the foreign-key validation pass. On a violation, removes the constraint on every node and
    /// deletes the job before rethrowing: a constraint that failed validation must neither stay
    /// half-rolled-out nor be retried by a resume, which would fail the same way.
    /// </summary>
    private async Task ValidateForeignKeyOrRemoveAsync(DatabaseDescriptor database, SchemaChangeJob job, SchemaDdlGateHold gate)
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
            // The removal proposes a delta and drops an index, so it runs under the gate, which the
            // validation pass released.
            await gate.AcquireAsync().ConfigureAwait(false);

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
    /// <para><b>Any failure takes the constraint back</b>, not only a violation. The wait or the read can
    /// also fail on a timeout or a lost leadership, and the statement then reports an error. A constraint
    /// left in place would be enforced and shown as valid over rows nobody proved, and the same statement
    /// could not run again, because its name would be taken. So the constraint is removed on every node
    /// and the job is deleted (<see cref="TakeBackRowConstraintAsync"/>). Only when the removal itself
    /// fails does the job stay, for the next leader. The original error is rethrown either way.</para>
    ///
    /// <para>After a read that passed, a failure to delete the job is logged, not thrown: the constraint
    /// is proven, and a job left behind only makes the next leader read the rows again.</para>
    ///
    /// <para>The table is held for the whole call and the DDL gate is released while the rows are read,
    /// exactly as <see cref="RunJobAsync"/> does for a backfill; <paramref name="gateHeldByCaller"/>
    /// has the same meaning as there.</para>
    /// </summary>
    public async Task ValidateRowConstraintAsync(DatabaseDescriptor database, SchemaChangeJob job, bool gateHeldByCaller = false)
    {
        SchemaDdlGateHold gate = new(database.DdlGate, gateHeldByCaller);
        SchemaDdlGate.StagedTableHold? tableHold = null;

        try
        {
            await gate.AcquireAsync().ConfigureAwait(false);
            tableHold = database.DdlGate.HoldTable(job.TableId, job.TableName, DescribeElement(job));

            try
            {
                if (RowConstraintValidationAsync is null)
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInternalOperation,
                        $"Constraint '{job.ElementName}' on table '{job.TableName}' cannot be validated: no row validation pass is wired");

                // The read proposes nothing and visits every row; see RunJobAsync.
                gate.Release();

                if (logger is not null)
                    Log.LogStagedChangeReleasesDdlGate(logger, DescribeElement(job), job.TableName, PhaseName(job.ElementKind), database.Name);

                await RequireEarlierWritersSettledAsync(database, job).ConfigureAwait(false);

                await RowConstraintValidationAsync(database, job.TableName, job.ElementKind, job.ElementName).ConfigureAwait(false);
            }
            catch
            {
                await gate.AcquireAsync().ConfigureAwait(false);
                await TakeBackRowConstraintAsync(database, job).ConfigureAwait(false);
                throw;
            }

            await gate.AcquireAsync().ConfigureAwait(false);

            try
            {
                await catalogs.DeleteCoordinatorJobAsync(database, job.TableId, job.ElementName).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex,
                    "Constraint {ElementName} on {TableName} was validated, but its job could not be deleted; the next leader validates it again",
                    job.ElementName, job.TableName);
            }
        }
        finally
        {
            tableHold?.Dispose();
            await gate.RestoreAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Removes a CHECK or NOT NULL constraint whose rows were not proven, then deletes its job. A
    /// constraint that is already gone needs no removal. Never throws: every caller is already
    /// failing or giving up, and its own error is the one to report.
    ///
    /// <para><b>The job is deleted only when the constraint is gone.</b> If the removal fails (for
    /// example, the node lost the schema leadership), the constraint is still enforced and still
    /// unproven, and the job is what makes the next leader finish it. A replication that committed
    /// but failed on its acknowledgements still counts as gone, because the local schema shows the
    /// removal.</para>
    ///
    /// <para>The caller holds the DDL gate: the removal proposes a delta.</para>
    /// </summary>
    internal async Task TakeBackRowConstraintAsync(DatabaseDescriptor database, SchemaChangeJob job)
    {
        try
        {
            await RemoveRowConstraintAsync(database, job).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger?.LogWarning(ex,
                "Could not remove the unproven {ElementKind} constraint {ElementName} on {TableName}",
                job.ElementKind, job.ElementName, job.TableName);
        }

        if (database.Schema.Tables.TryGetValue(job.TableName, out TableSchema? table)
            && RowConstraintExists(table, job.ElementKind, job.ElementName))
        {
            logger?.LogError(
                "The unproven {ElementKind} constraint {ElementName} on {TableName} is still enforced; its job stays for the next schema leader",
                job.ElementKind, job.ElementName, job.TableName);
            return;
        }

        try
        {
            await catalogs.DeleteCoordinatorJobAsync(database, job.TableId, job.ElementName).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger?.LogWarning(ex,
                "Could not delete the job of constraint {ElementName} on {TableName}; the next leader finds no constraint and deletes it",
                job.ElementName, job.TableName);
        }
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

    private static PersistedCoordinatorJob BuildPersistedJob(SchemaChangeJob job, ColumnInfo? columnDefinition, IndexBuildInfo? indexBuildInfo, int attempts, long leaderTerm) => new()
    {
        LeaderTerm = leaderTerm,
        TableName = job.TableName,
        TableId = job.TableId,
        ElementName = job.ElementName,
        TargetState = job.TargetState,
        ElementKind = job.ElementKind,
        ColumnType = columnDefinition?.Type,
        ColumnNotNull = columnDefinition?.NotNull ?? false,
        ColumnDefault = columnDefinition?.Default,
        ColumnMaxLength = columnDefinition?.MaxLength,
        ColumnArrayElementType = columnDefinition?.ArrayElementType,
        ColumnDefaultFunction = columnDefinition?.DefaultFunction,
        ColumnDefaultSequenceId = columnDefinition?.DefaultSequenceId,
        ColumnIdentityAlways = columnDefinition?.IdentityAlways ?? false,
        ColumnNotNullConstraintName = columnDefinition?.NotNullConstraintName,
        ColumnComment = columnDefinition?.Comment,
        ColumnStorage = columnDefinition?.Storage,
        IndexId = indexBuildInfo?.IndexId,
        IndexColumnIds = indexBuildInfo?.ColumnIds,
        IndexType = indexBuildInfo?.IndexType,
        IndexIncludeColumnIds = indexBuildInfo?.IncludeColumnIds,
        IndexColumnDirections = indexBuildInfo?.ColumnDirections,
        Attempts = attempts,
    };

    /// <summary>
    /// Resumes the persisted jobs of <paramref name="database"/> as the schema leader callback does
    /// (<see cref="CoordinatorResumeTrigger.LeaderElected"/>).
    /// </summary>
    public Task ResumeJobsAsync(DatabaseDescriptor database)
        => ResumeJobsAsync(database, CoordinatorResumeTrigger.LeaderElected);

    /// <summary>
    /// Loads all persisted coordinator jobs for <paramref name="database"/> and drives each to its
    /// target state. Called when this node wins the schema leadership, when a database opens on a node
    /// that leads already, on the leader's periodic sweep, and by a statement waiting for a job it
    /// handed over (<see cref="CoordinatorResumeTrigger"/>).
    ///
    /// <para>One resume runs at a time per database. A call that finds one running returns at once,
    /// and the running resume reads the jobs again before it ends, so a job recorded or made visible
    /// while it ran is not missed. The caller that was turned away therefore cannot assume the work is
    /// done when its call returns; a caller that needs the outcome polls the record, as the handover
    /// wait does.</para>
    /// </summary>
    public async Task ResumeJobsAsync(DatabaseDescriptor database, CoordinatorResumeTrigger trigger)
    {
        ResumeRun run = resumeRuns.GetOrAdd(database.Id, static _ => new ResumeRun());

        while (true)
        {
            if (trigger == CoordinatorResumeTrigger.LeaderElected)
                Interlocked.Exchange(ref run.AwaitVisibility, 1);

            Interlocked.Exchange(ref run.Pending, 1);

            // Somebody else runs the resume: it reads Pending before it lets the gate go, and goes
            // round once more for this request.
            if (!await run.Gate.WaitAsync(0).ConfigureAwait(false))
                return;

            try
            {
                while (Interlocked.Exchange(ref run.Pending, 0) == 1)
                {
                    bool awaitVisibility = Interlocked.Exchange(ref run.AwaitVisibility, 0) == 1;
                    await ResumeJobsOnceAsync(database, trigger, awaitVisibility).ConfigureAwait(false);
                }
            }
            finally
            {
                run.Gate.Release();
            }

            // A request that landed between the last read of Pending and the release was turned
            // away by the gate; it is served by going round again. At worst one extra resume runs,
            // and it finds nothing to do.
            if (Volatile.Read(ref run.Pending) == 0)
                return;
        }
    }

    /// <summary>The single-flight state of one database's resume, see <see cref="resumeRuns"/>.</summary>
    private sealed class ResumeRun
    {
        public readonly SemaphoreSlim Gate = new(1, 1);

        /// <summary>1 while a resume is requested and not yet started.</summary>
        public int Pending;

        /// <summary>1 when a pending request came from the leader election and wants the empty list re-read.</summary>
        public int AwaitVisibility;
    }

    /// <summary>The key of a job in <see cref="retryNotBefore"/>.</summary>
    private static string RetryKey(DatabaseDescriptor database, string tableId, string elementName)
        => string.Concat(database.Id, "|", tableId, "|", elementName);

    /// <summary>
    /// Records that an attempt of a job failed on this node, so the next resume leaves it alone until
    /// its backoff is over. The wait doubles with the attempts the record counts, from
    /// <see cref="ResumeRetryBackoff"/> up to <see cref="ResumeRetryBackoffMax"/>.
    /// </summary>
    private void RecordFailedAttempt(DatabaseDescriptor database, PersistedCoordinatorJob persisted, string tableName)
    {
        double factor = Math.Pow(2, Math.Clamp(persisted.Attempts - 1, 0, 16));
        TimeSpan backoff = TimeSpan.FromTicks((long)Math.Min(ResumeRetryBackoffMax.Ticks, ResumeRetryBackoff.Ticks * factor));

        retryNotBefore[RetryKey(database, persisted.TableId, persisted.ElementName)] = DateTime.UtcNow.Add(backoff);

        if (logger is not null)
            Log.LogResumeRetryScheduled(logger, tableName, persisted.ElementName, persisted.TargetState, database.Name, persisted.Attempts, backoff);
    }

    /// <summary>
    /// True when a job failed on this node recently enough that this resume must not drive it yet.
    /// </summary>
    private bool IsInBackoff(DatabaseDescriptor database, PersistedCoordinatorJob persisted, string tableName)
    {
        string key = RetryKey(database, persisted.TableId, persisted.ElementName);

        if (!retryNotBefore.TryGetValue(key, out DateTime notBefore))
            return false;

        if (DateTime.UtcNow < notBefore)
        {
            if (logger is not null)
                Log.LogResumeDeferredByBackoff(logger, tableName, persisted.ElementName, database.Name, notBefore);

            return true;
        }

        retryNotBefore.TryRemove(key, out _);
        return false;
    }

    /// <summary>Forgets the backoff of a job that finished, was deleted, or belongs to another leader now.</summary>
    private void ClearBackoff(DatabaseDescriptor database, PersistedCoordinatorJob persisted)
        => retryNotBefore.TryRemove(RetryKey(database, persisted.TableId, persisted.ElementName), out _);

    /// <summary>
    /// Number of times the leader-election resume re-reads an empty job list, 100 ms apart. The
    /// leader callback fires before the KV state machine has applied every committed entry on the new
    /// leader, so the record written by the previous driver may not be visible on the first read.
    /// </summary>
    private const int VisibilityReadAttempts = 10;

    /// <summary>One resume, see <see cref="ResumeJobsAsync(DatabaseDescriptor, CoordinatorResumeTrigger)"/>.</summary>
    private async Task ResumeJobsOnceAsync(DatabaseDescriptor database, CoordinatorResumeTrigger trigger, bool awaitVisibility)
    {
        int readAttempts = awaitVisibility ? VisibilityReadAttempts : 1;
        List<PersistedCoordinatorJob> jobs = [];

        for (int attempt = 0; attempt < readAttempts; attempt++)
        {
            try
            {
                jobs = await catalogs.LoadCoordinatorJobsAsync(database).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // The sweep, or the next trigger, reads again.
                logger?.LogWarning(ex, "Failed to load coordinator jobs for database {DbName} on {Trigger} resume; skipping", database.Name, trigger);
                return;
            }

            if (jobs.Count > 0 || attempt == readAttempts - 1)
                break;

            await Task.Delay(100).ConfigureAwait(false);
        }

        if (jobs.Count == 0)
        {
            // Said out loud on every trigger but the sweep, so a resume that saw nothing shows in the
            // log next to the election or the open that caused it.
            if (trigger != CoordinatorResumeTrigger.Sweep && logger is not null)
                Log.LogNoCoordinatorJobsToResume(logger, database.Name, trigger);

            return;
        }

        // Every record write of this resume carries the term this node leads in now, so a driver that
        // lost the leadership to this node is refused at its next write (CoordinatorJobStore).
        long term = ReadLeaderTerm(database);
        LeaderTerm = term;

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

            // A table a staged change holds on this node is being driven right now, by the statement
            // that started it: this node kept or regained the leadership while the job ran. Driving the
            // job a second time would run two backfills over the same rows and fight over the job
            // record; the running statement finishes it, or its compensation removes it.
            if (database.DdlGate.IsTableHeld(liveTableName))
            {
                if (logger is not null)
                    Log.LogResumeSkipsHeldTable(logger, liveTableName, persisted.ElementName, database.Name);
                continue;
            }

            // A job that failed on this node a moment ago waits its backoff out; the sweep comes back.
            if (IsInBackoff(database, persisted, liveTableName))
                continue;

            // A CHECK or NOT NULL job applies its own attempt limit: abandoning it must remove the
            // constraint, not just delete the job.
            if (persisted.ElementKind is SchemaElementKind.Check or SchemaElementKind.NotNull)
            {
                await ResumeRowConstraintJobAsync(database, persisted, liveTable, job, term).ConfigureAwait(false);
                continue;
            }

            // Abandon a job that keeps failing across leader changes rather than retry it on
            // every election forever. A terminal failure (unreachable invariant, persistent
            // validation error) burns one attempt per resume; once the budget is spent we
            // delete + log loudly instead of looping.
            if (persisted.Attempts >= MaxResumeAttempts)
            {
                logger?.LogError(
                    "Abandoning coordinator job {TableName}.{ElementName} → {TargetState} on database {DbName} after {Attempts} resume attempts; " +
                    "the {ElementKind} stays in its current state and must be dropped by hand before it can be created again",
                    liveTableName, persisted.ElementName, persisted.TargetState, database.Name, persisted.Attempts, persisted.ElementKind);
                try { await catalogs.DeleteCoordinatorJobAsync(database, persisted.TableId, persisted.ElementName).ConfigureAwait(false); }
                catch (Exception ex) { logger?.LogWarning(ex, "Failed to delete abandoned coordinator job for database {DbName}", database.Name); }
                ClearBackoff(database, persisted);
                continue;
            }

            // A column job whose target is Absent records an ADD COLUMN that failed. It is finished by
            // removing the column, never by adding it, and a removal is always safe to run again. Its
            // steps propose deltas, so it runs under the gate, like every proposal.
            if (persisted.ElementKind == SchemaElementKind.Column && persisted.TargetState == SchemaElementState.Absent)
            {
                SchemaDdlGateHold removalGate = new(database.DdlGate, heldByCaller: false);
                try
                {
                    await removalGate.AcquireAsync().ConfigureAwait(false);
                    await RemoveColumnAsync(database, job, persisted.ColumnDefaultSequenceId, term).ConfigureAwait(false);
                }
                finally
                {
                    removalGate.Release();
                }
                continue;
            }

            ColumnInfo? columnDefinition = persisted.ColumnType.HasValue
                ? new ColumnInfo(
                    persisted.ElementName,
                    persisted.ColumnType.Value,
                    persisted.ColumnNotNull,
                    persisted.ColumnDefault,
                    maxLength: persisted.ColumnMaxLength,
                    arrayElementType: persisted.ColumnArrayElementType,
                    defaultFunction: persisted.ColumnDefaultFunction,
                    notNullConstraintName: persisted.ColumnNotNullConstraintName,
                    comment: persisted.ColumnComment,
                    storage: persisted.ColumnStorage,
                    defaultSequenceId: persisted.ColumnDefaultSequenceId,
                    identityAlways: persisted.ColumnIdentityAlways)
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

            // The resume holds nothing when it starts; the gate is taken for the state read and for each
            // delta, and released for the backfill, exactly as RunJobAsync does for a statement.
            SchemaDdlGateHold gate = new(database.DdlGate, heldByCaller: false);

            try
            {
                await gate.AcquireAsync().ConfigureAwait(false);

                SchemaElementState current = GetCurrentElementState(database.Schema, liveTableName, job.ElementName, job.ElementKind);
                SchemaElementState[] path = ComputePathFor(job.ElementKind, current, job.TargetState);

                if (path.Length == 0)
                {
                    // Already at target — e.g. the previous leader completed the last step but
                    // crashed before deleting the record. Clean it up rather than leave it.
                    await catalogs.DeleteCoordinatorJobAsync(database, persisted.TableId, job.ElementName).ConfigureAwait(false);
                    ClearBackoff(database, persisted);
                    continue;
                }

                // Record this resume attempt durably BEFORE driving, so a crash mid-resume still
                // counts against the budget and the job can't be retried indefinitely. The write
                // stamps this node's term on the record: from here on the previous driver's writes
                // are refused, and a resume of a leader newer than this node refuses this one.
                persisted.Attempts++;
                persisted.LeaderTerm = term;
                await catalogs.PersistCoordinatorJobAsync(database, persisted).ConfigureAwait(false);

                if (logger is not null)
                    Log.LogResumingCoordinatorJob(logger, liveTableName, persisted.ElementName, persisted.TargetState, database.Name, persisted.Attempts);

                await DriveToTargetAsync(database, job, columnDefinition, indexBuildInfo, persisted.StartOffset, current, path, persisted.Attempts, term, gate, CancellationToken.None)
                    .ConfigureAwait(false);

                ClearBackoff(database, persisted);
            }
            catch (CoordinatorJobTakenOverException ex)
            {
                logger?.LogInformation(ex.Message + "; this resume leaves the job to that leader");
                ClearBackoff(database, persisted);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex,
                    "Coordinator resume failed for {TableName}.{ElementName} → {TargetState} on database {DbName}",
                    liveTableName, persisted.ElementName, persisted.TargetState, database.Name);
                RecordFailedAttempt(database, persisted, liveTableName);
            }
            finally
            {
                gate.Release();
            }
        }
    }

    /// <summary>
    /// Finishes a CHECK or NOT NULL job that a previous leader left: the constraint is enforced, but its
    /// existing rows were not proven. A job whose constraint is gone is deleted. Otherwise the attempt
    /// is recorded first, as for every resumed job, and the rows are validated. A failure is logged,
    /// not thrown, so one job cannot stop the resume of the others.
    ///
    /// <para><b>An abandoned job removes its constraint.</b> Once <see cref="MaxResumeAttempts"/> is
    /// spent, the rows are not read again, and the constraint is taken back
    /// (<see cref="TakeBackRowConstraintAsync"/>). Deleting only the job, as an index or foreign-key
    /// job does, would be wrong here: those elements stay in a non-public state that shows the
    /// problem, but a CHECK or NOT NULL has no state, so it would stay enforced and shown as valid over
    /// rows nobody proved. A job outlives a successful statement only when its delete failed after the
    /// read passed; then each resume reads rows that already satisfy the constraint and finishes, so
    /// in practice an abandoned job belongs to a statement that reported an error.</para>
    /// </summary>
    private async Task ResumeRowConstraintJobAsync(DatabaseDescriptor database, PersistedCoordinatorJob persisted, TableSchema liveTable, SchemaChangeJob job, long term)
    {
        try
        {
            if (!RowConstraintExists(liveTable, persisted.ElementKind, persisted.ElementName))
            {
                await catalogs.DeleteCoordinatorJobAsync(database, persisted.TableId, persisted.ElementName).ConfigureAwait(false);
                return;
            }

            if (persisted.Attempts >= MaxResumeAttempts)
            {
                logger?.LogError(
                    "Abandoning {ElementKind} constraint {TableName}.{ElementName} on database {DbName} after {Attempts} resume attempts; the constraint is removed",
                    persisted.ElementKind, job.TableName, persisted.ElementName, database.Name, persisted.Attempts);

                SchemaDdlGateHold removalGate = new(database.DdlGate, heldByCaller: false);
                try
                {
                    await removalGate.AcquireAsync().ConfigureAwait(false);
                    await TakeBackRowConstraintAsync(database, job).ConfigureAwait(false);
                }
                finally
                {
                    removalGate.Release();
                }
                return;
            }

            persisted.Attempts++;
            persisted.LeaderTerm = term;
            await catalogs.PersistCoordinatorJobAsync(database, persisted).ConfigureAwait(false);

            if (logger is not null)
                Log.LogResumingCoordinatorJob(logger, job.TableName, persisted.ElementName, persisted.TargetState, database.Name, persisted.Attempts);

            await ValidateRowConstraintAsync(database, job).ConfigureAwait(false);
            ClearBackoff(database, persisted);
        }
        catch (Exception ex)
        {
            logger?.LogError(ex,
                "Coordinator resume failed to validate {ElementKind} constraint {TableName}.{ElementName} on database {DbName}",
                persisted.ElementKind, job.TableName, persisted.ElementName, database.Name);
            RecordFailedAttempt(database, persisted, job.TableName);
        }
    }

    /// <summary>
    /// Maximum number of resume attempts before a job is abandoned. An attempt is one pickup, by a
    /// leader election, an open, the sweep or a handover wait. A transient failure (a leadership flap,
    /// a peer down during the ack wait) normally completes within one or two attempts; exhausting
    /// this budget means the job is genuinely stuck, so it is deleted and logged rather than retried
    /// forever. The sweep retries a failed job after a backoff that doubles per attempt
    /// (<see cref="ResumeRetryBackoff"/>, <see cref="ResumeRetryBackoffMax"/>), so with the defaults
    /// a job that fails every time is abandoned after some six minutes of retries, not after six
    /// ticks. A CHECK or NOT NULL job also removes its constraint at this point
    /// (<see cref="ResumeRowConstraintJobAsync"/>).
    /// </summary>
    internal const int MaxResumeAttempts = 8;

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
