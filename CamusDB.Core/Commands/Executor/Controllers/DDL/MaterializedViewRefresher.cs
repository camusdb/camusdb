
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Replaces a materialized view's contents by <b>building them in a fresh key-space and adopting
/// it</b>, rather than by emptying and refilling the one readers are using.
///
/// <para>Two things rule out the obvious "delete every row, then insert the new ones" implementation.
/// A transaction may not exceed <see cref="CamusDBOptions.MaxMutationsPerTransaction"/>, which any
/// materialized view worth materializing would blow through; and a concurrent reader would observe
/// the relation mid-rewrite — empty, or half old and half new — which is a wrong answer, not a slow
/// one. Building somewhere nobody can reach and then adopting it in a single replicated schema change
/// avoids both: the rebuild is as many transactions as it needs, and the switch-over is atomic across
/// the cluster.</para>
///
/// <para>What is adopted is the <b>storage</b>, not the identity. The materialized view keeps its
/// relation id across a refresh (see <see cref="Catalogs.Models.TableSchema.StorageId"/>), because
/// that id is what privilege grants, dependency edges, cached results and statistics are keyed by —
/// a refresh that changed it would silently revoke every grant on the view, as though the object had
/// been dropped and recreated. It was not; only its contents were replaced.</para>
///
/// <para>It also makes readers strictly better off than under PostgreSQL's non-concurrent
/// <c>REFRESH</c>, which takes an exclusive lock and blocks them. Here a reader that started before
/// the swap simply keeps reading the previous relation at its own snapshot, and one that starts after
/// it sees the new contents whole. Nobody waits.</para>
///
/// <para>The rebuild reads its source at <b>one pinned snapshot</b> for its whole duration. Reading
/// each chunk at "now" would assemble the result from several different instants, producing a
/// relation whose contents never existed together — the kind of wrong answer that is impossible to
/// notice from the data alone.</para>
///
/// <para><b>Failure is designed to be uneventful.</b> Everything before the swap touches only the
/// staging relation, so a failed or abandoned refresh leaves the materialized view exactly as it was;
/// the staging relation is destroyed on the way out, and a run that dies without getting that far
/// leaves one behind that the next refresh of the same view — or the background sweep — reclaims. An
/// interrupted run is then <b>restarted</b>, not continued: see
/// <see cref="TakeOverAbandonedRefreshAsync"/> for why picking up mid-scan is not the same thing.</para>
/// </summary>
internal sealed class MaterializedViewRefresher
{
    /// <summary>
    /// Test-only seam: invoked once the staging relation exists but before the rebuild reads or
    /// publishes anything, so a test can commit competing DDL exactly inside the window a real race
    /// would occupy — deterministically, without threads or sleeps. Null in production.
    /// </summary>
    internal static Func<Task>? AfterStagingForTesting { get; set; }

    /// <summary>The points of a rebuild at which <see cref="FaultInjectionForTesting"/> runs.</summary>
    internal enum FaultPoint
    {
        /// <summary>The staging relation exists and nothing is loaded.</summary>
        AfterStaging,

        /// <summary>A chunk of rows was committed into the staging relation.</summary>
        AfterChunk,

        /// <summary>Every chunk was committed; nothing is published.</summary>
        AfterFinalChunk,

        /// <summary>The swap is committed, applied and checkpointed; the ownership record is not yet deleted.</summary>
        AfterPublish,

        /// <summary>A failed rebuild removed its staging relation and has not finished cleaning up.</summary>
        DuringCleanup,
    }

    /// <summary>
    /// Test-only seam: invoked at each <see cref="FaultPoint"/> of a rebuild. Throwing
    /// <see cref="SimulatedCrashForTesting"/> from it stands in for the process dying there: the
    /// rebuild stops at once and runs none of the cleanup a failure would, exactly as a crash would
    /// not. Null in production.
    /// </summary>
    internal static Func<FaultPoint, Task>? FaultInjectionForTesting { get; set; }

    /// <summary>A process death at a <see cref="FaultPoint"/>; see <see cref="FaultInjectionForTesting"/>.</summary>
    internal sealed class SimulatedCrashForTesting(FaultPoint point) : Exception($"simulated crash at {point}")
    {
        public FaultPoint Point { get; } = point;
    }

    private static Task InjectAsync(FaultPoint point)
        => FaultInjectionForTesting is { } inject ? inject(point) : Task.CompletedTask;

    /// <summary>
    /// Runs a parsed <c>REFRESH MATERIALIZED VIEW</c> statement and returns the number of rows
    /// materialized.
    /// </summary>
    /// <remarks>
    /// Shared by the non-query path, where the statement belongs because it writes rows, and the DDL
    /// path, which accepts it as well: clients classify statements by their first words, and one that
    /// counts <c>REFRESH MATERIALIZED VIEW</c> as view DDL — camus-cli did — sends it to the DDL
    /// endpoint. A statement that works through one entry point and is "unknown" through the other is,
    /// to that client, a statement the server does not support.
    /// </remarks>
    internal Task<int> RefreshStatementAsync(
        CommandExecutor executor,
        CatalogsManager catalogs,
        Task<DatabaseRegistry> registryTask,
        DatabaseDescriptor database,
        NodeAst ast,
        ExecuteSQLTicket ticket,
        ILogger<ICamusDB> logger)
    {
        string options = ast.yytext ?? "";

        return RefreshAsync(
            executor, catalogs, registryTask, database, ast.leftAst!.yytext!,
            concurrently: options.StartsWith("concurrently", StringComparison.Ordinal),
            withNoData: options.EndsWith("no data", StringComparison.Ordinal),
            ticket, logger);
    }

    /// <summary>
    /// Rebuilds <paramref name="viewName"/> and returns the number of rows materialized.
    /// </summary>
    /// <param name="withNoData">
    /// Empties the materialized view and marks it unpopulated instead of loading rows. It still goes
    /// through the swap: the contents have to be replaced atomically whether the replacement holds
    /// rows or not.
    /// </param>
    internal async Task<int> RefreshAsync(
        CommandExecutor executor,
        CatalogsManager catalogs,
        Task<DatabaseRegistry> registryTask,
        DatabaseDescriptor database,
        string viewName,
        bool concurrently,
        bool withNoData,
        ExecuteSQLTicket ticket,
        ILogger<ICamusDB> logger)
    {
        if (concurrently)
            throw new CamusDBException(
                CamusDBErrorCodes.FeatureNotSupported,
                "REFRESH MATERIALIZED VIEW CONCURRENTLY is not supported. CamusDB's ordinary refresh " +
                "already leaves readers unblocked, which is what CONCURRENTLY buys in PostgreSQL; what " +
                "it additionally does — write only the rows that changed — is not implemented. Run " +
                "REFRESH MATERIALIZED VIEW without CONCURRENTLY.");

        if (!database.Options.MaterializedViewRefreshEnabled)
            throw new CamusDBException(
                CamusDBErrorCodes.FeatureNotSupported,
                "Materialized view refresh is disabled on this node (materialized_view_refresh_enabled)");

        TableSchema view = RequireMaterializedView(database, viewName);

        // The target is authorized explicitly, because nothing else authorizes it. A refresh never
        // opens the materialized view — it builds a separate relation and swaps the storage in — so
        // the per-table chokepoint that guards every ordinary write is never reached, and mapping the
        // statement to a privilege enforces nothing on its own. Checked before any staging state is
        // created so a refused refresh leaves nothing behind.
        RequireTargetPrivilege(database, viewName, view);

        RequireDefinition(view, viewName);

        string viewTableId = view.Id!;

        DatabaseRegistry registry = await registryTask.ConfigureAwait(false);

        // A leased, Raft-replicated fence keyed by database and relation id — the same mechanism relink
        // and orphan reclamation use. The gate this replaces was a dictionary in one executor, so it
        // fenced nothing across nodes: two could refresh the same view at once, and each would sweep
        // the other's staging relation out from under it.
        string fenceId = RegistryDropMarkers.TableFenceId(database.Id, viewTableId);

        if (BeforeRefreshFenceForTesting is { } beforeFence)
            await beforeFence(viewName).ConfigureAwait(false);

        if (!await AcquireRefreshFenceAsync(registry, fenceId).ConfigureAwait(false))
            throw new CamusDBException(
                CamusDBErrorCodes.RefreshAlreadyInProgress,
                $"A REFRESH of materialized view '{viewName}' is already running; wait for it to finish and retry");

        try
        {
            // Read again by id now that the fence is held. A drop can take the fence first, drop the
            // view and release it while this statement waited; the copy read before the fence would
            // then stage rows for a view that is gone, and the swap would have nothing to publish onto.
            view = FindRelationById(database, viewTableId) is { IsMaterializedView: true } current
                ? current
                : throw new CamusDBException(
                    CamusDBErrorCodes.ViewDoesntExist,
                    $"Materialized view '{viewName}' was dropped before its refresh started; nothing was refreshed");
            viewName = view.Name ?? viewName;

            // Safe only because the fence is held: a run still in progress anywhere would still hold
            // it, so whatever is found here belongs to one that is gone. The user asked for this
            // rebuild, so it simply subsumes the abandoned one rather than taking it over — which is
            // also why the takeover counter starts again at zero.
            await ReclaimAbandonedRefreshAsync(executor, catalogs, database, viewTableId, logger).ConfigureAwait(false);

            return await RunFencedRefreshAsync(
                executor, catalogs, registry, database, view, viewName, withNoData,
                takeoverAttempts: 0, ticket, fenceId, logger, CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            // Released on every path so the next refresh does not wait out the lease. A process that
            // dies here leaves the fence to expire on its own, which is the point of it being leased.
            await registry.DropMarkers.ReleaseDropIntentAsync(fenceId).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Test-only seam: invoked with the view's name after a <c>REFRESH</c> resolved the view and before
    /// it takes the refresh fence, so a test can drop the view inside that window. Null in production.
    /// </summary>
    internal static Func<string, Task>? BeforeRefreshFenceForTesting { get; set; }

    /// <summary>
    /// The relation with id <paramref name="relationId"/>, or null when no relation has it. Reads the
    /// id index, which every applied delta keeps current, so it is safe without the schema lock.
    /// </summary>
    private static TableSchema? FindRelationById(DatabaseDescriptor database, string relationId)
        => database.Schema.TryGetRelationNameById(relationId, out string? name)
            && database.Schema.Tables.TryGetValue(name, out TableSchema? relation)
            && string.Equals(relation.Id, relationId, StringComparison.Ordinal)
                ? relation
                : null;

    /// <summary>
    /// Builds the new contents and publishes them, with the refresh fence already held by the caller
    /// and nothing of an earlier attempt left to clean up.
    /// </summary>
    /// <remarks>
    /// Shared by the two things that can legitimately rebuild a materialized view: the user's
    /// <c>REFRESH</c>, and a background takeover of a run that was interrupted. They differ only in
    /// how authority was established and in what <paramref name="takeoverAttempts"/> records — the
    /// build, the compare-and-swap and the ownership record must be identical, because a takeover that
    /// published by a slightly different route would be a second, less-tested swap implementation.
    /// </remarks>
    /// <param name="takeoverAttempts">
    /// Stamped onto this attempt's ownership record so a takeover of <em>this</em> run knows how many
    /// restarts have already been spent. Zero for a refresh a user asked for.
    /// </param>
    private static async Task<int> RunFencedRefreshAsync(
        CommandExecutor executor,
        CatalogsManager catalogs,
        DatabaseRegistry registry,
        DatabaseDescriptor database,
        TableSchema view,
        string viewName,
        bool withNoData,
        int takeoverAttempts,
        ExecuteSQLTicket ticket,
        string fenceId,
        ILogger<ICamusDB> logger,
        CancellationToken cancellationToken)
    {
        ViewDefinition definition = RequireDefinition(view, viewName);
        string viewTableId = view.Id!;

        // Captured before anything is staged, so it covers the whole window in which the copied layout
        // could go stale — the swap refuses to publish if the definition moved.
        long metadataGeneration = view.MetadataGeneration;

        string stagingTableId = await registry.AllocateTableIdAsync().ConfigureAwait(false);
        string stagingName = MaterializedViewNaming.StagingRelationName(viewTableId, stagingTableId);

        RefreshRun run = new(takeoverAttempts > 0 ? "takeover" : "refresh", ObjectIdGenerator.Generate().ToString(), takeoverAttempts);

        // Recorded before the relation exists. A record naming a relation that was never created is
        // inert; a relation created with no record naming it is storage nothing can find.
        await catalogs.PersistRefreshJobAsync(database, new MaterializedViewRefreshJob
        {
            JobId = run.JobId,
            DatabaseId = database.Id,
            ViewTableId = viewTableId,
            ViewName = viewName,
            StagingTableId = stagingTableId,
            StagingName = stagingName,
            ExpectedMetadataGeneration = metadataGeneration,
            Owner = fenceId,
            StartedAt = executor.ClusterNow(),
            WithNoData = withNoData,
            TakeoverAttempts = takeoverAttempts,
        }).ConfigureAwait(false);

        await executor.CreateRelationInDdlTransactionAsync(
            database, BuildStagingTicket(database.Name, stagingName, view), stagingTableId,
            validate: false).ConfigureAwait(false);

        try
        {
            if (AfterStagingForTesting is not null)
                await AfterStagingForTesting().ConfigureAwait(false);

            await InjectAsync(FaultPoint.AfterStaging).ConfigureAwait(false);

            HLCTimestamp snapshot = executor.ClusterNow();
            run.Phase = "loading";
            run.Source = snapshot;

            int rows = withNoData
                ? 0
                : await LoadAsync(
                    executor, database, definition, stagingName, view.Name ?? viewName, view.Indexes?.Count ?? 0,
                    snapshot, ticket, run, cancellationToken).ConfigureAwait(false);

            run.Phase = "publishing";
            run.Publish = executor.ClusterNow();

            // One replicated change publishes the rebuilt relation, records how stale it now is,
            // and detaches the relation it replaced. Everything up to here was invisible.
            await catalogs.SetMaterializedViewStateAsync(
                database,
                tableId: viewTableId,
                isPopulated: !withNoData,
                // How stale the contents are is a property of the SOURCE READ, so it keeps the
                // snapshot the rebuild pinned...
                refreshedAt: withNoData ? null : snapshot,
                swapToTableId: stagingTableId,
                // ...but when this change happened is now, not when the rebuild started. Stamping
                // the entry with the source snapshot would order it before operations that
                // preceded it, and — because the retired storage's orphan record takes its
                // DroppedAt from here — would spend a long rebuild's entire duration out of that
                // storage's retention window before it was even detached.
                publishHlc: run.Publish.Value,
                expectedMetadataGeneration: metadataGeneration).ConfigureAwait(false);

            // The swap applies as a no-op when the view is gone, and then the staging relation is
            // still a relation of its own. Raised as a failure, so the cleanup below removes it.
            if (database.Schema.Tables.TryGetValue(stagingName, out TableSchema? unswapped)
                && string.Equals(unswapped.Id, stagingTableId, StringComparison.Ordinal))
                throw new CamusDBException(
                    CamusDBErrorCodes.ViewDoesntExist,
                    $"Materialized view '{viewName}' was dropped while it was being refreshed; nothing was published");

            run.Phase = "published";
            await InjectAsync(FaultPoint.AfterPublish).ConfigureAwait(false);

            // The staging relation has become the materialized view, so nothing is left to own.
            // Deleted after the swap, never before: a record removed while the relation it names
            // is still separate storage would strand exactly what it exists to make findable.
            await catalogs.DeleteRefreshJobAsync(database, viewTableId).ConfigureAwait(false);

            // The rows were counted while they were written, but under the staging relation's id —
            // which the swap has just retired. Move those counts onto the view, whose own were
            // evicted by the same apply because they described the contents this replaced. Skipping
            // this leaves the view costed by fallback constants, and unable to look stale to the
            // background collector, so it would stay uncosted until someone ran ANALYZE by hand.
            //
            // After the swap and after the job record is gone: the refresh is already published and
            // durable at this point, so a failure here must not undo any of it. Statistics are
            // advisory, so the worst case is the behavior that existed before this ran at all.
            await AdoptRefreshStatisticsAsync(
                executor, database, viewName, stagingTableId, rows, logger).ConfigureAwait(false);

            // No cache invalidation here on purpose: the replicated apply does it on every node,
            // including this one. Doing it only at the proposer is what left followers answering
            // from the contents the swap retired.
            Report(logger, database, viewName, run, error: null);
            return rows;
        }
        catch (Exception error) when (error is not SimulatedCrashForTesting)
        {
            Report(logger, database, viewName, run, error);

            // The refresh can have failed because the statement ran out of time; the cleanup must not
            // be stopped by the same deadline.
            StatementDeadline.Suspend();

            // The swap never happened, so the materialized view still holds its previous contents
            // and the only thing worth removing is the half-built relation. A failure to remove it
            // must not replace the caller's real error: it costs storage, nothing more, and the
            // next refresh of this view sweeps it.
            try
            {
                await executor.DropStagingRelationAsync(database, stagingName).ConfigureAwait(false);
                await InjectAsync(FaultPoint.DuringCleanup).ConfigureAwait(false);

                // A view that is gone has no rebuild left to finish, so its record goes with the
                // storage it named.
                if (FindRelationById(database, viewTableId) is null)
                    await catalogs.DeleteRefreshJobAsync(database, viewTableId).ConfigureAwait(false);
            }
            catch (SimulatedCrashForTesting)
            {
                throw;
            }
            catch (Exception cleanupError)
            {
                // Logs the error code only: a message can quote a row value.
                logger.LogWarning(
                    "REFRESH of materialized view '{ViewName}' failed and its staging relation could not be removed ({ErrorCode}); it will be swept by the next refresh",
                    viewName, ErrorCode(cleanupError));
            }

            // For a view that still exists, the ownership record is deliberately left in place. It is
            // what the sweep reads to find this run's storage if the drop above did not happen, and
            // what tells it a rebuild was interrupted rather than never started.
            throw;
        }
    }

    /// <summary>
    /// Creates a materialized view <c>WITH DATA</c>: loads the body's rows into a staging relation
    /// nobody can see, then creates the view on that storage, already populated, in one replicated
    /// change. Returns the number of rows materialized.
    /// </summary>
    /// <remarks>
    /// <para><b>One publication.</b> Creating the view first and populating it in a second
    /// step published an object the statement had not finished: other sessions and nodes could list it,
    /// query it, grant on it or alter it while it was empty, and a failed population then dropped
    /// something they might already have touched. Now nothing is published until the rows are in place,
    /// so no statement anywhere can observe the view half-created.</para>
    ///
    /// <para><b>Recovery needs no help from this process.</b> The fence is taken on the view's id before
    /// anything is staged, and the ownership record is written before the staging relation exists —
    /// the same order a refresh uses. If the process dies before the view is published, the sweep finds
    /// the record, finds no view with that id, and removes the staging relation: the view simply never
    /// existed, and a retry of the statement starts clean. If it dies after, the sweep sees the view
    /// already owns that storage and removes only the record.</para>
    ///
    /// <para>The name is not held while the rows load. A relation created under it meanwhile wins, and
    /// this statement then fails with the name taken, leaving nothing behind.</para>
    /// </remarks>
    internal async Task<int> CreatePopulatedAsync(
        CommandExecutor executor,
        CatalogsManager catalogs,
        Task<DatabaseRegistry> registryTask,
        DatabaseDescriptor database,
        string viewName,
        string viewTableId,
        CreateTableTicket viewTicket,
        ExecuteSQLTicket ticket,
        ILogger<ICamusDB> logger)
    {
        if (!database.Options.MaterializedViewRefreshEnabled)
            throw new CamusDBException(
                CamusDBErrorCodes.FeatureNotSupported,
                "Materialized view refresh is disabled on this node (materialized_view_refresh_enabled); " +
                "create the materialized view WITH NO DATA and refresh it from another node");

        ViewDefinition definition = viewTicket.ViewDefinition
            ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "A materialized view needs a definition");

        DatabaseRegistry registry = await registryTask.ConfigureAwait(false);
        string fenceId = RegistryDropMarkers.TableFenceId(database.Id, viewTableId);

        // A fresh id, so nobody else can hold this; taken anyway, so the sweep cannot mistake the
        // record written below for an abandoned one while this statement is still running.
        if (!await AcquireRefreshFenceAsync(registry, fenceId).ConfigureAwait(false))
            throw new CamusDBException(
                CamusDBErrorCodes.RefreshAlreadyInProgress,
                $"Materialized view '{viewName}' is already being created; retry later");

        try
        {
            string stagingTableId = await registry.AllocateTableIdAsync().ConfigureAwait(false);
            string stagingName = MaterializedViewNaming.StagingRelationName(viewTableId, stagingTableId);

            RefreshRun run = new("create", ObjectIdGenerator.Generate().ToString(), 0);

            await catalogs.PersistRefreshJobAsync(database, new MaterializedViewRefreshJob
            {
                JobId = run.JobId,
                DatabaseId = database.Id,
                ViewTableId = viewTableId,
                ViewName = viewName,
                StagingTableId = stagingTableId,
                StagingName = stagingName,
                Owner = fenceId,
                StartedAt = executor.ClusterNow(),
            }).ConfigureAwait(false);

            // An ordinary table with the view's own layout: the rows are written into it, and writing
            // into a materialized view is refused. It becomes the view's storage when it is adopted.
            await executor.CreateRelationInDdlTransactionAsync(
                database,
                new CreateTableTicket(
                    databaseName: database.Name,
                    tableName: stagingName,
                    columns: viewTicket.Columns,
                    constraints: viewTicket.Constraints,
                    ifNotExists: false,
                    checkConstraints: viewTicket.CheckConstraints,
                    settings: viewTicket.Settings),
                stagingTableId,
                validate: false).ConfigureAwait(false);

            try
            {
                if (AfterStagingForTesting is not null)
                    await AfterStagingForTesting().ConfigureAwait(false);

                await InjectAsync(FaultPoint.AfterStaging).ConfigureAwait(false);

                HLCTimestamp snapshot = executor.ClusterNow();
                int indexCount = database.Schema.Tables.TryGetValue(stagingName, out TableSchema? staging)
                    ? staging.Indexes?.Count ?? 0
                    : 0;

                run.Phase = "loading";
                run.Source = snapshot;

                int rows = await LoadAsync(
                    executor, database, definition, stagingName, viewName, indexCount, snapshot, ticket,
                    run, CancellationToken.None).ConfigureAwait(false);

                run.Phase = "publishing";
                run.Publish = executor.ClusterNow();

                // The one change that publishes the view: created on the staging storage, populated,
                // with the staging relation gone.
                await executor.CreateRelationInDdlTransactionAsync(
                    database,
                    new CreateTableTicket(
                        databaseName: viewTicket.DatabaseName,
                        tableName: viewTicket.TableName,
                        columns: viewTicket.Columns,
                        constraints: viewTicket.Constraints,
                        ifNotExists: false,
                        checkConstraints: viewTicket.CheckConstraints,
                        comment: viewTicket.Comment,
                        settings: viewTicket.Settings,
                        kind: RelationKind.MaterializedView,
                        viewDefinition: definition)
                    {
                        AdoptStagingTableId = stagingTableId,
                        RefreshedAt = snapshot,
                    },
                    viewTableId,
                    validate: true).ConfigureAwait(false);

                run.Phase = "published";
                await InjectAsync(FaultPoint.AfterPublish).ConfigureAwait(false);

                await catalogs.DeleteRefreshJobAsync(database, viewTableId).ConfigureAwait(false);

                await AdoptRefreshStatisticsAsync(executor, database, viewName, stagingTableId, rows, logger).ConfigureAwait(false);

                Report(logger, database, viewName, run, error: null);
                return rows;
            }
            catch (Exception error) when (error is not SimulatedCrashForTesting)
            {
                Report(logger, database, viewName, run, error);

                // Nothing was published, so the staging relation is all there is to remove. The record
                // goes only once that worked: while the storage exists, the record is what finds it.
                try
                {
                    await executor.DropStagingRelationAsync(database, stagingName).ConfigureAwait(false);
                    await catalogs.DeleteRefreshJobAsync(database, viewTableId).ConfigureAwait(false);
                }
                catch (Exception cleanupError)
                {
                    logger.LogWarning(
                        "CREATE MATERIALIZED VIEW of '{ViewName}' failed and its staging relation could not be removed ({ErrorCode}); the background sweep will remove it",
                        viewName, ErrorCode(cleanupError));
                }

                throw;
            }
        }
        finally
        {
            await registry.DropMarkers.ReleaseDropIntentAsync(fenceId).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// What one rebuild did, for its telemetry: the phase it reached, the work it wrote, the snapshot
    /// it read and when it published, and how its snapshot hold fared.
    /// </summary>
    internal sealed class RefreshRun(string kind, string jobId, int attempt)
    {
        public string Kind { get; } = kind;

        public string JobId { get; } = jobId;

        public int Attempt { get; } = attempt;

        public string Phase { get; set; } = "staging";

        public int Rows { get; set; }

        public long Mutations { get; set; }

        public long Bytes { get; set; }

        public HLCTimestamp? Source { get; set; }

        public HLCTimestamp? Publish { get; set; }

        public int HoldRenewals { get; set; }

        public bool HoldLost { get; set; }

        public System.Diagnostics.Stopwatch Watch { get; } = System.Diagnostics.Stopwatch.StartNew();
    }

    /// <summary>
    /// Exports how a rebuild ended: the totals of <see cref="Diagnostics.MaterializedViewRefreshMetrics"/>,
    /// the server meter, and one log line. Never the view's SQL and never a row value — a view body and
    /// its rows are user data — and for a failure the error code rather than its message, which can quote
    /// a value.
    /// </summary>
    private static void Report(ILogger<ICamusDB> logger, DatabaseDescriptor database, string viewName, RefreshRun run, Exception? error)
    {
        string outcome = error switch
        {
            null => "published",
            CamusDBException { Code: CamusDBErrorCodes.ConcurrentSchemaChange } => "conflict",
            _ => "failed",
        };

        Diagnostics.MaterializedViewRefreshMetrics.RecordRebuild(outcome, run.Rows, run.Mutations, run.Bytes, run.HoldRenewals, run.HoldLost);
        Diagnostics.ServerDiagnostics.RecordMatViewRefresh(run.Kind, outcome, run.Rows, run.Mutations, run.Bytes, run.Watch.Elapsed.TotalMilliseconds);

        if (error is null)
        {
            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation(
                    "Materialized view {Kind} published: database '{DatabaseName}', view '{ViewName}', job {JobId}, attempt {Attempt}, " +
                    "rows {Rows}, mutations {Mutations}, bytes {Bytes}, source {SourceHlc}, publish {PublishHlc}, " +
                    "hold renewals {HoldRenewals}, elapsed {ElapsedMs} ms",
                    run.Kind, database.Name, viewName, run.JobId, run.Attempt, run.Rows, run.Mutations, run.Bytes,
                    run.Source, run.Publish, run.HoldRenewals, (long)run.Watch.Elapsed.TotalMilliseconds);
            return;
        }

        logger.LogWarning(
            "Materialized view {Kind} {Outcome} with {ErrorCode} in phase {Phase}: database '{DatabaseName}', view '{ViewName}', " +
            "job {JobId}, attempt {Attempt}, rows {Rows}, source {SourceHlc}, hold lost {HoldLost}, elapsed {ElapsedMs} ms",
            run.Kind, outcome, ErrorCode(error), run.Phase, database.Name, viewName, run.JobId, run.Attempt, run.Rows,
            run.Source, run.HoldLost, (long)run.Watch.Elapsed.TotalMilliseconds);
    }

    /// <summary>The code of a CamusDB error, or the type of any other; never the message.</summary>
    private static string ErrorCode(Exception error) => error is CamusDBException camus ? camus.Code : error.GetType().Name;

    /// <summary>The stored body a rebuild reads, or a corruption error naming the view that lost it.</summary>
    private static ViewDefinition RequireDefinition(TableSchema view, string viewName)
        => view.ViewDefinition
            ?? throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Materialized view '{viewName}' has no stored definition and cannot be refreshed");

    /// <summary>
    /// Streams the view body at <paramref name="snapshot"/> into the staging relation, committing
    /// every <see cref="CamusDBOptions.MaterializedViewRefreshChunkRows"/> rows.
    /// </summary>
    /// <param name="cancellationToken">
    /// Cancels a rebuild the engine started on its own behalf, so node shutdown does not wait for a
    /// background takeover to finish scanning. Never cancelled for a user's <c>REFRESH</c>: that one
    /// belongs to the caller, who is waiting for its answer.
    /// </param>
    private static async Task<int> LoadAsync(
        CommandExecutor executor,
        DatabaseDescriptor database,
        ViewDefinition definition,
        string stagingName,
        string viewName,
        int indexCount,
        HLCTimestamp snapshot,
        ExecuteSQLTicket ticket,
        RefreshRun run,
        CancellationToken cancellationToken)
    {
        NodeAst bodyAst = SQLParserProcessor.Parse(definition.Sql);

        // Same resolution a query-time expansion does: the stored body names its sources by
        // immutable id, so a rebuild reads whatever those ids point at now rather than whatever they
        // were called when the view was created.
        if (StoredBodyBinder.MayContainReferences(definition.Sql))
            bodyAst = StoredBodyBinder.ResolveStoredForm(database.Schema, bodyAst, viewName);

        await using SelectRowSource source = await executor
            .BuildMaterializedViewSourceAsync(database, bodyAst, ticket, snapshot).ConfigureAwait(false);


        // The relation carries one more column than the body projects — the synthesized key every
        // relation needs — and it is deliberately left out of the target list so each row takes its
        // generated default, exactly as CREATE TABLE ... AS SELECT does.
        List<string> targetColumns = [.. (definition.Columns ?? []).Select(column => column.Name)];

        int chunkRows = ResolveChunkRows(database.Options, indexCount);
        List<QueryResultRow> buffer = new(Math.Min(chunkRows, 1024));
        int total = 0;

        await foreach (QueryResultRow row in source.Cursor.ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            // If the pin is lost the remaining rows would be read against a floor that has already
            // moved, so the drain stops rather than finishing with a result quietly missing history.
            source.ThrowIfSnapshotLost("this materialized view refresh");

            buffer.Add(row);
            run.Bytes += Cache.QueryResultCache.EstimateRowBytes(row);

            if (buffer.Count < chunkRows)
                continue;

            total += await executor.InsertRefreshChunkAsync(
                database, stagingName, source.Columns, targetColumns, buffer).ConfigureAwait(false);
            buffer.Clear();

            await InjectAsync(FaultPoint.AfterChunk).ConfigureAwait(false);
        }

        if (buffer.Count > 0)
        {
            total += await executor.InsertRefreshChunkAsync(
                database, stagingName, source.Columns, targetColumns, buffer).ConfigureAwait(false);

            await InjectAsync(FaultPoint.AfterChunk).ConfigureAwait(false);
        }

        await InjectAsync(FaultPoint.AfterFinalChunk).ConfigureAwait(false);

        run.Rows = total;
        run.Mutations = (long)total * (1 + indexCount);
        run.HoldRenewals = source.HoldRenewals;
        run.HoldLost = source.HoldLost;

        // The last word before the caller publishes: a hold lost after the final chunk is just as
        // damaging as one lost mid-scan, and the drain finishing is not evidence it survived.
        source.ThrowIfSnapshotLost("this materialized view refresh");

        return total;
    }

    /// <summary>
    /// How many rows one rebuild transaction may carry.
    ///
    /// <para>The configured chunk size counts <b>rows</b>, but the per-transaction ceiling counts
    /// <b>mutations</b>, and a row is never one mutation: it writes itself plus an entry in every
    /// index. With the synthesized primary key alone the 10,000-row default already reaches the
    /// 20,000-mutation default exactly, and one secondary index puts it over — so a materialized view
    /// with any index of its own would fail to refresh on a stock configuration once it grew past a
    /// few thousand rows. Taking the row figure as a ceiling rather than a target, and deriving the
    /// real one from the index count, is what keeps the configured value meaningful without letting it
    /// promise something the transaction layer will refuse.</para>
    ///
    /// <para>The budget is deliberately fractional rather than exact. Index maintenance is not always
    /// one write per index per row — a non-unique index writes a differently-shaped entry, and an
    /// update-shaped path can touch an entry twice — so aiming at the cap would leave nothing for the
    /// cases that cost more than the estimate.</para>
    /// </summary>
    private static int ResolveChunkRows(CamusDBOptions options, int indexCount)
    {
        int configured = Math.Max(1, options.MaterializedViewRefreshChunkRows);
        int mutationLimit = options.MaxMutationsPerTransaction;

        // Zero (or negative) disables the ceiling entirely; then the configured row count is the only
        // thing bounding a chunk, which is what the operator asked for.
        if (mutationLimit <= 0)
            return configured;

        // The row itself, plus one entry per index. Indexes are null only on a relation that predates
        // replicated index definitions, which a materialized view never is; the +1 for the primary key
        // is already included because it is an ordinary entry in that list.
        int mutationsPerRow = 1 + indexCount;

        int affordable = (int)Math.Max(1, (long)mutationLimit * MutationBudgetPercent / 100 / mutationsPerRow);

        return Math.Min(configured, affordable);
    }

    /// <summary>
    /// Share of <see cref="CamusDBOptions.MaxMutationsPerTransaction"/> a rebuild chunk may plan to
    /// use. The remainder absorbs index writes that cost more than the flat per-index estimate.
    /// </summary>
    private const int MutationBudgetPercent = 70;

    /// <summary>
    /// Takes the refresh fence, retrying briefly before giving up.
    /// </summary>
    /// <remarks>
    /// The retry is not about contention between two refreshes — a real one holds the fence for the
    /// whole rebuild, so waiting a moment would not help and reporting it immediately is right. It is
    /// about the <b>background reclaimer</b>, which takes the same fence for a few milliseconds while
    /// it checks whether a job is abandoned. Without this, a user's REFRESH could be refused because a
    /// janitor happened to be mid-sweep, which is an outcome no caller can act on or predict.
    ///
    /// <para><c>DROP MATERIALIZED VIEW</c> takes the fence through here too, for the same reason: it
    /// must not be refused because a janitor is mid-sweep, and it must be refused while a real rebuild
    /// is running.</para>
    /// </remarks>
    internal static async Task<bool> AcquireRefreshFenceAsync(DatabaseRegistry registry, string fenceId)
    {
        for (int attempt = 0; ; attempt++)
        {
            if (await registry.DropMarkers.AcquireDropIntentAsync(fenceId).ConfigureAwait(false))
                return true;

            if (attempt >= FenceAcquireAttempts - 1)
                return false;

            await Task.Delay(FenceAcquireBackoffMs << attempt).ConfigureAwait(false);
        }
    }

    /// <summary>Attempts to take the refresh fence before reporting it as held.</summary>
    private const int FenceAcquireAttempts = 4;

    /// <summary>First backoff between fence attempts; doubles each time (25/50/100 ms).</summary>
    private const int FenceAcquireBackoffMs = 25;

    /// <summary>
    /// Drops the staging relation left behind by an earlier refresh of this materialized view, and the
    /// record that owned it.
    /// </summary>
    /// <remarks>
    /// Driven by the persisted job record rather than by scanning names, so it removes exactly the
    /// relation the abandoned attempt created instead of anything that merely looks like one.
    ///
    /// <para><b>The caller must hold the refresh fence.</b> That is what makes this safe: a run still
    /// in progress — on this node or any other — holds the fence, so a record visible here can only
    /// belong to one that is gone. Sweeping without the fence is precisely how two concurrent refreshes
    /// used to delete each other's storage.</para>
    /// </remarks>
    internal static async Task ReclaimAbandonedRefreshAsync(
        CommandExecutor executor,
        CatalogsManager catalogs,
        DatabaseDescriptor database,
        string viewTableId,
        ILogger<ICamusDB> logger)
    {
        MaterializedViewRefreshJob? abandoned =
            await catalogs.TryGetRefreshJobAsync(database, viewTableId).ConfigureAwait(false);

        if (abandoned is null)
            return;

        logger.LogWarning(
            "Reclaiming the staging relation of an unfinished materialized view refresh (job {JobId}) in database '{DatabaseName}'",
            abandoned.JobId, database.Name);

        await executor.DropStagingRelationAsync(database, abandoned.StagingName).ConfigureAwait(false);
        await catalogs.DeleteRefreshJobAsync(database, viewTableId).ConfigureAwait(false);

        Diagnostics.MaterializedViewRefreshMetrics.RecordReclaim("staging_dropped");
    }

    /// <summary>
    /// Finishes what an interrupted refresh started: reclaims the storage the dead run was building
    /// into and then <b>runs the rebuild again from the beginning</b>, so exactly one node completes
    /// it. Returns whether a record was found and dealt with.
    ///
    /// <para><b>Restart, not resume.</b> Continuing from a progress cursor means re-running the body
    /// at the pinned snapshot and skipping the first K rows, which is only correct if the body yields
    /// rows in the same order on every execution. That holds for a single-relation scan and is not
    /// promised for a join, an aggregate, or anything the planner may order differently — and a
    /// resumed non-deterministic body silently produces a materialized view with duplicated and
    /// missing rows. Rebuilding costs the work the dead run had already done; resuming would cost
    /// correctness, on a class of query nobody can identify from the outside.</para>
    ///
    /// <para><b>The caller must hold the refresh fence</b>, which is what proves the run that wrote
    /// the record is gone, and what keeps this the only rebuild in the cluster while it runs. It is
    /// held for the whole restarted rebuild, not just for the reclamation — which means a user's
    /// <c>REFRESH</c> issued mid-restart is refused with <c>RefreshAlreadyInProgress</c>. That is the
    /// one place a sweep is allowed to turn a user statement away, and it is not the janitor case the
    /// brief fence-acquire retry exists for: a rebuild really is running, it is the one that user's
    /// view needs, and the message tells them to wait for it. The alternative — letting the two run
    /// at once — is what the fence exists to prevent.</para>
    ///
    /// <para><b>Bounded.</b> Every restart is counted on the durable record before anything is
    /// destroyed, so a rebuild that keeps failing gives up after
    /// <see cref="CamusDBOptions.MaterializedViewRefreshTakeoverAttempts"/> attempts and leaves the
    /// view holding its previous contents rather than looping forever behind a janitor nobody is
    /// watching.</para>
    ///
    /// <para><b>It runs with the engine's authority, not a session's.</b> There is no caller to
    /// authorize: the statement this continues was authorized when it was issued, possibly on a node
    /// that no longer exists. A restart therefore performs no privilege check of its own — it can only
    /// re-run a body already stored on the view, into the view that body belongs to, so it grants
    /// nobody access to anything a user could not already reach through that view.</para>
    /// </summary>
    internal static async Task<bool> TakeOverAbandonedRefreshAsync(
        CommandExecutor executor,
        CatalogsManager catalogs,
        DatabaseRegistry registry,
        DatabaseDescriptor database,
        string viewTableId,
        string fenceId,
        ILogger<ICamusDB> logger,
        CancellationToken cancellationToken)
    {
        // Re-read under the fence. The listing that led here was taken before the fence was acquired,
        // so it may name a run that has since finished.
        MaterializedViewRefreshJob? abandoned =
            await catalogs.TryGetRefreshJobAsync(database, viewTableId).ConfigureAwait(false);

        if (abandoned is null)
            return false;

        TableSchema? view = await FindMaterializedViewByIdAsync(database, viewTableId).ConfigureAwait(false);

        // The swap committed and only the record's removal was lost. What this record names is no
        // longer staging storage — it is the materialized view's own key-space — so reclaiming it
        // would destroy live contents, and restarting the rebuild would redo work that is already
        // published. Drop the record and nothing else.
        if (view is not null
            && string.Equals(view.EffectiveStorageId, abandoned.StagingTableId, StringComparison.Ordinal))
        {
            await catalogs.DeleteRefreshJobAsync(database, viewTableId).ConfigureAwait(false);
            Diagnostics.MaterializedViewRefreshMetrics.RecordReclaim("record_only");
            return true;
        }

        int spent = abandoned.TakeoverAttempts;
        int allowed = database.Options.MaterializedViewRefreshTakeoverAttempts;

        // A record that names another database describes a run this database never started — it
        // could only have arrived by a branch fork copying its parent's namespace. Nothing about it
        // is abandoned work of THIS database, so the rebuild it would trigger is one nobody asked
        // for, run against base tables that may already hold post-fork writes. The record and any
        // storage that was copied with it are removed; the view keeps the contents it forked with.
        bool foreign = !string.IsNullOrEmpty(abandoned.DatabaseId)
            && !string.Equals(abandoned.DatabaseId, database.Id, StringComparison.Ordinal);

        if (foreign)
            logger.LogWarning(
                "Refresh job {JobId} found in database '{DatabaseName}' belongs to database id {OwnerDatabaseId}; removing the record without restarting its rebuild",
                abandoned.JobId, database.Name, abandoned.DatabaseId);

        bool restart =
            !foreign
            && view is not null
            && database.Options.MaterializedViewRefreshEnabled
            && spent < allowed
            && !cancellationToken.IsCancellationRequested;

        if (restart)
        {
            // Counted first, on the record that still owns the dead run's storage. Incrementing after
            // the storage is dropped — or holding the count in memory — would reset it every time a
            // takeover died the same way the run before it did, and the bound would never be reached.
            abandoned.TakeoverAttempts = spent + 1;
            await catalogs.PersistRefreshJobAsync(database, abandoned).ConfigureAwait(false);
        }

        logger.LogWarning(
            "Reclaiming the staging relation of an unfinished materialized view refresh (job {JobId}) in database '{DatabaseName}'",
            abandoned.JobId, database.Name);

        await executor.DropStagingRelationAsync(database, abandoned.StagingName).ConfigureAwait(false);

        if (!restart)
        {
            await catalogs.DeleteRefreshJobAsync(database, viewTableId).ConfigureAwait(false);

            Diagnostics.MaterializedViewRefreshMetrics.RecordReclaim(
                foreign ? "foreign" : view is not null && spent >= allowed && allowed > 0 ? "gave_up" : "staging_dropped");

            if (view is not null && spent >= allowed && allowed > 0)
                logger.LogError(
                    "Materialized view '{ViewName}' in database '{DatabaseName}' was left stale: its refresh was restarted {Attempts} times and never completed. Its previous contents are intact; issue REFRESH MATERIALIZED VIEW to try again.",
                    view.Name, database.Name, spent);

            return true;
        }

        // A restart is engine work, not a continuation of whoever's session started it — that session
        // may have been on a node that no longer exists. Clearing the ambient scope says so explicitly
        // rather than relying on the background sweep never having had one.
        AuthorizationScope callerScope = AuthorizationContext.Current;
        AuthorizationContext.Current = default;

        Diagnostics.MaterializedViewRefreshMetrics.RecordReclaim("restarted");

        try
        {
            int rows = await RunFencedRefreshAsync(
                executor, catalogs, registry, database, view!, view!.Name ?? abandoned.ViewName,
                abandoned.WithNoData, spent + 1, TakeoverTicket(executor, database, view!),
                fenceId, logger, cancellationToken).ConfigureAwait(false);

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation(
                    "Restarted the interrupted refresh of materialized view '{ViewName}' in database '{DatabaseName}' ({Rows} rows, attempt {Attempt} of {Allowed})",
                    view!.Name, database.Name, rows, spent + 1, allowed);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Shutdown. The record this attempt wrote survives, so the next sweep — here or on
            // whichever node is elected next — picks the view up again.
        }
        catch (Exception error)
        {
            // Never propagated: this is a janitor finishing somebody else's statement, and a failure
            // here must not stop the sweep from reaching the other databases. The attempt is already
            // counted, so a rebuild that fails every time still terminates.
            logger.LogWarning(
                "Restarting the interrupted refresh of materialized view '{ViewName}' in database '{DatabaseName}' failed with {ErrorCode} (attempt {Attempt} of {Allowed})",
                view!.Name, database.Name, ErrorCode(error), spent + 1, allowed);
        }
        finally
        {
            AuthorizationContext.Current = callerScope;
        }

        return true;
    }

    /// <summary>
    /// The statement context a restarted rebuild runs under. It names no caller and carries no
    /// parameters — the body is read from the view's stored definition, which cannot reference a
    /// placeholder — and its transaction is a read-only snapshot handle that the refresh path replaces
    /// with its own pinned one before reading anything.
    /// </summary>
    private static ExecuteSQLTicket TakeoverTicket(
        CommandExecutor executor, DatabaseDescriptor database, TableSchema view)
        => new(
            Transactions.KvTransaction.CreateSnapshotReadOnly(executor.ClusterNow()),
            database.Name,
            $"REFRESH MATERIALIZED VIEW {view.Name}",
            parameters: null);

    /// <summary>
    /// Resolves a materialized view by its immutable id, under the schema lock.
    /// </summary>
    /// <remarks>
    /// By id rather than by the name the job record carries, because a rename between the interrupted
    /// run and this one would otherwise either miss the view or — worse — find a different relation
    /// that has since taken the freed name. Null means it was dropped, which is the one case where
    /// there is nothing left to rebuild.
    /// </remarks>
    private static async Task<TableSchema?> FindMaterializedViewByIdAsync(DatabaseDescriptor database, string viewTableId)
    {
        // The lock is taken and released before any KV write: the schema lock may not be held across
        // one, and holding it for the length of a rebuild would block every DDL statement in the
        // database behind a janitor.
        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            foreach (TableSchema table in database.Schema.Tables.Values)
            {
                if (table.IsMaterializedView && string.Equals(table.Id, viewTableId, StringComparison.Ordinal))
                    return table;
            }

            return null;
        }
        finally
        {
            database.Schema.ReleaseLock();
        }
    }

    /// <summary>
    /// Hands the counts measured during the population to the materialized view now that it owns the
    /// key-space they describe.
    ///
    /// <para>Deliberately failure-tolerant: it runs after the swap has been published and the job
    /// record deleted, so the refresh has already succeeded and nothing here may undo it. A failure
    /// leaves the view with no statistics — which is exactly where it stood before, and which
    /// <c>ANALYZE</c> repairs.</para>
    /// </summary>
    private static async Task AdoptRefreshStatisticsAsync(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string viewName,
        string stagingTableId,
        int rows,
        ILogger<ICamusDB> logger)
    {
        try
        {
            // Opened after the swap, so this is the view bound to its new key-space — the descriptor
            // the apply evicted is not reused.
            TableDescriptor view = await executor.OpenTableWithDescriptor(
                database, new OpenTableTicket(database.Name, viewName)).ConfigureAwait(false);

            await executor.Statistics.AdoptRefreshedRelationAsync(
                database, stagingTableId, view, rows).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Publishing refreshed statistics for materialized view '{ViewName}' failed; it will be "
                + "costed without them until an ANALYZE runs",
                viewName);
        }
    }

    /// <summary>
    /// Derives the staging relation from the materialized view's <b>own</b> current layout rather than
    /// from the body's output schema.
    ///
    /// <para>That choice is what makes a refresh a contents-only operation. Re-deriving the shape
    /// would let a column added to a base table silently widen the materialized view — the same drift
    /// a non-materialized view's frozen column list exists to prevent — and would leave every
    /// dependent object bound to a shape that changed without a statement being issued against it.
    /// Copying the layout also copies the column ids, which a view that reads the materialized view
    /// stores in its body, and the indexes, so the rebuilt relation is as queryable as the one it
    /// replaces the instant it is swapped in.</para>
    ///
    /// <para>It is created as an ordinary table, not a materialized view: the rebuild writes rows into
    /// it, and writing into a materialized view is refused. It becomes one at the swap.</para>
    /// </summary>
    private static CreateTableTicket BuildStagingTicket(string databaseName, string stagingName, TableSchema view)
    {
        ColumnInfo[] columns = [.. (view.Columns ?? []).Select(column => new ColumnInfo(
            name: column.Name,
            type: column.Type,
            notNull: column.NotNull,
            defaultValue: column.DefaultValue,
            maxLength: column.MaxLength,
            integerBits: column.IntegerBits,
            arrayElementType: column.ArrayElementType,
            defaultFunction: column.DefaultFunction,
            notNullConstraintName: column.NotNullConstraintName,
            comment: column.Comment,
            storage: column.Storage,
            defaultSequenceId: column.DefaultSequenceId,
            identityAlways: column.IdentityAlways)
        {
            // Kept, so the swap leaves the view's column ids as they were and a view that reads this
            // materialized view still finds every column it stored.
            PreservedId = column.Id,
        })];

        Dictionary<string, string> nameByColumnId = new(StringComparer.Ordinal);
        foreach (TableColumnSchema column in view.Columns ?? [])
            nameByColumnId[column.Id] = column.Name;

        List<ConstraintInfo> constraints = [];

        foreach (TableIndexSchema index in view.Indexes ?? [])
        {
            string[] columnNames = ResolveColumnNames(index.ColumnIds, index.Columns, nameByColumnId);
            if (columnNames.Length == 0)
                continue;

            ConstraintType type = string.Equals(index.Name, CamusDBConstants.PrimaryKeyInternalName, StringComparison.Ordinal)
                ? ConstraintType.PrimaryKey
                : index.Type switch
                {
                    IndexType.Unique => ConstraintType.IndexUnique,
                    IndexType.Inverted => ConstraintType.IndexInverted,
                    _ => ConstraintType.IndexMulti,
                };

            ColumnIndexInfo[] indexColumns = new ColumnIndexInfo[columnNames.Length];
            for (int i = 0; i < columnNames.Length; i++)
                indexColumns[i] = new ColumnIndexInfo(
                    columnNames[i],
                    index.ColumnDirections is { Length: > 0 } directions && i < directions.Length
                        ? directions[i]
                        : OrderType.Ascending);

            constraints.Add(new ConstraintInfo(
                type,
                index.Name,
                indexColumns,
                ResolveColumnNames(index.IncludeColumnIds, index.IncludeColumns, nameByColumnId),
                index.Comment));
        }

        CheckConstraintInfo[] checks = [.. (view.CheckConstraints ?? []).Select(check => new CheckConstraintInfo(
            check.Name, check.Expression, check.ReferencedColumns))];

        return new CreateTableTicket(
            databaseName: databaseName,
            tableName: stagingName,
            columns: columns,
            constraints: [.. constraints],
            ifNotExists: false,
            checkConstraints: checks,
            settings: view.Settings);
    }

    /// <summary>
    /// Resolves an index's columns to names, preferring the persisted column ids and falling back to
    /// the resolved names an in-memory descriptor projection carries.
    /// </summary>
    private static string[] ResolveColumnNames(
        string[]? columnIds, string[] columnNames, Dictionary<string, string> nameByColumnId)
    {
        if (columnIds is not { Length: > 0 })
            return columnNames;

        string[] resolved = new string[columnIds.Length];
        for (int i = 0; i < columnIds.Length; i++)
        {
            if (!nameByColumnId.TryGetValue(columnIds[i], out string? name))
                throw new CamusDBException(
                    CamusDBErrorCodes.SystemSpaceCorrupt,
                    $"Index column '{columnIds[i]}' does not resolve to a column of the materialized view");

            resolved[i] = name;
        }

        return resolved;
    }

    /// <summary>
    /// Checks the caller against the materialized view itself for the privilege the statement mapped
    /// to, mirroring what <c>TableOpener.Open</c> would have done had this path opened the relation.
    /// For every statement that reaches a materialized view without opening it: a refresh, and
    /// <c>SHOW CREATE MATERIALIZED VIEW</c>.
    /// </summary>
    /// <remarks>
    /// Fails closed as the table opener does: a user's request that declares no required privilege
    /// comes from an entry point that forgot to publish one, and is refused rather than waved through.
    /// </remarks>
    internal static void RequireTargetPrivilege(DatabaseDescriptor database, string viewName, TableSchema view)
    {
        if (!database.Options.AuthenticationEnabled)
            return;

        AuthorizationScope scope = AuthorizationContext.Current;

        if (scope.Principal is null)
            return;

        if (scope.RequiredPrivilege is not { } required)
            throw new CamusDBException(
                CamusDBErrorCodes.InsufficientPrivilege,
                $"Access to materialized view '{database.Name}.{viewName}' was refused: the request did not " +
                "declare which privilege it needs, so it cannot be authorized");

        if (!scope.Principal.HasPrivilege(required, database.Id, view.Id))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InsufficientPrivilege,
                $"Missing {required} privilege on materialized view '{database.Name}.{viewName}'");
        }
    }

    /// <summary>
    /// Resolves a name to a materialized view, distinguishing the two ways it can fail to be one so
    /// the caller is told which statement to use instead.
    /// </summary>
    internal static TableSchema RequireMaterializedView(DatabaseDescriptor database, string name)
    {
        if (database.Schema.Views.ContainsKey(name))
            throw new CamusDBException(
                CamusDBErrorCodes.ViewDoesntExist,
                $"'{name}' is a view, not a materialized view; it holds no data of its own and is evaluated on every read");

        if (!database.Schema.Tables.TryGetValue(name, out TableSchema? relation))
            throw new CamusDBException(
                CamusDBErrorCodes.ViewDoesntExist, $"Materialized view '{name}' does not exist");

        if (!relation.IsMaterializedView)
            throw new CamusDBException(
                CamusDBErrorCodes.ViewDoesntExist, $"'{name}' is a table, not a materialized view");

        return relation;
    }
}
