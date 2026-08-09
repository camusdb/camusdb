
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
/// leaves one behind that the next refresh of the same view sweeps. What is <em>not</em> implemented
/// is resumption: a refresh interrupted by a crash or a leadership change is not picked up where it
/// stopped, it simply has to be run again.</para>
/// </summary>
internal sealed class MaterializedViewRefresher
{
    /// <summary>
    /// Test-only seam: invoked once the staging relation exists but before the rebuild reads or
    /// publishes anything, so a test can commit competing DDL exactly inside the window a real race
    /// would occupy — deterministically, without threads or sleeps. Null in production.
    /// </summary>
    internal static Func<Task>? AfterStagingForTesting { get; set; }

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

        ViewDefinition definition = view.ViewDefinition
            ?? throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Materialized view '{viewName}' has no stored definition and cannot be refreshed");

        string viewTableId = view.Id!;

        // Captured before anything is staged, so it covers the whole window in which the copied layout
        // could go stale — the swap refuses to publish if the definition moved.
        long metadataGeneration = view.MetadataGeneration;

        DatabaseRegistry registry = await registryTask.ConfigureAwait(false);

        // A leased, Raft-replicated fence keyed by database and relation id — the same mechanism relink
        // and orphan reclamation use. The gate this replaces was a dictionary in one executor, so it
        // fenced nothing across nodes: two could refresh the same view at once, and each would sweep
        // the other's staging relation out from under it.
        string fenceId = DatabaseRegistry.TableFenceId(database.Id, viewTableId);

        if (!await AcquireRefreshFenceAsync(registry, fenceId).ConfigureAwait(false))
            throw new CamusDBException(
                CamusDBErrorCodes.RefreshAlreadyInProgress,
                $"A REFRESH of materialized view '{viewName}' is already running; wait for it to finish and retry");

        try
        {
            // Safe only because the fence is held: a run still in progress anywhere would still hold
            // it, so whatever is found here belongs to one that is gone.
            await ReclaimAbandonedRefreshAsync(executor, catalogs, database, viewTableId, logger).ConfigureAwait(false);

            string stagingTableId = await registry.AllocateTableIdAsync().ConfigureAwait(false);
            string stagingName = MaterializedViewNaming.StagingRelationName(viewTableId, stagingTableId);

            // Recorded before the relation exists. A record naming a relation that was never created is
            // inert; a relation created with no record naming it is storage nothing can find.
            await catalogs.PersistRefreshJobAsync(database, new MaterializedViewRefreshJob
            {
                JobId = ObjectIdGenerator.Generate().ToString(),
                ViewTableId = viewTableId,
                ViewName = viewName,
                StagingTableId = stagingTableId,
                StagingName = stagingName,
                ExpectedMetadataGeneration = metadataGeneration,
                Owner = fenceId,
                StartedAt = executor.ClusterNow(),
            }).ConfigureAwait(false);

            await executor.CreateRelationInDdlTransactionAsync(
                database, BuildStagingTicket(database.Name, stagingName, view), stagingTableId,
                validate: false).ConfigureAwait(false);

            try
            {
                if (AfterStagingForTesting is not null)
                    await AfterStagingForTesting().ConfigureAwait(false);

                HLCTimestamp snapshot = executor.ClusterNow();
                int rows = withNoData
                    ? 0
                    : await LoadAsync(executor, database, definition, stagingName, view, snapshot, ticket).ConfigureAwait(false);

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
                    publishHlc: executor.ClusterNow(),
                    expectedMetadataGeneration: metadataGeneration).ConfigureAwait(false);

                // The staging relation has become the materialized view, so nothing is left to own.
                // Deleted after the swap, never before: a record removed while the relation it names
                // is still separate storage would strand exactly what it exists to make findable.
                await catalogs.DeleteRefreshJobAsync(database, viewTableId).ConfigureAwait(false);

                // No cache invalidation here on purpose: the replicated apply does it on every node,
                // including this one. Doing it only at the proposer is what left followers answering
                // from the contents the swap retired.
                return rows;
            }
            catch (Exception)
            {
                // The swap never happened, so the materialized view still holds its previous contents
                // and the only thing worth removing is the half-built relation. A failure to remove it
                // must not replace the caller's real error: it costs storage, nothing more, and the
                // next refresh of this view sweeps it.
                try
                {
                    await executor.DropStagingRelationAsync(database, stagingName).ConfigureAwait(false);
                }
                catch (Exception cleanupError)
                {
                    logger.LogWarning(
                        cleanupError,
                        "REFRESH of materialized view '{ViewName}' failed and its staging relation could not be removed; it will be swept by the next refresh",
                        viewName);
                }

                throw;
            }
        }
        finally
        {
            // Released on every path so the next refresh does not wait out the lease. A process that
            // dies here leaves the fence to expire on its own, which is the point of it being leased.
            await registry.ReleaseDropIntentAsync(fenceId).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Streams the view body at <paramref name="snapshot"/> into the staging relation, committing
    /// every <see cref="CamusDBOptions.MaterializedViewRefreshChunkRows"/> rows.
    /// </summary>
    private static async Task<int> LoadAsync(
        CommandExecutor executor,
        DatabaseDescriptor database,
        ViewDefinition definition,
        string stagingName,
        TableSchema view,
        HLCTimestamp snapshot,
        ExecuteSQLTicket ticket)
    {
        NodeAst bodyAst = SQLParserProcessor.Parse(definition.Sql);

        await using SelectRowSource source = await executor
            .BuildMaterializedViewSourceAsync(database, bodyAst, ticket, snapshot).ConfigureAwait(false);


        // The relation carries one more column than the body projects — the synthesized key every
        // relation needs — and it is deliberately left out of the target list so each row takes its
        // generated default, exactly as CREATE TABLE ... AS SELECT does.
        List<string> targetColumns = [.. (definition.Columns ?? []).Select(column => column.Name)];

        int chunkRows = ResolveChunkRows(database.Options, view);
        List<QueryResultRow> buffer = new(Math.Min(chunkRows, 1024));
        int total = 0;

        await foreach (QueryResultRow row in source.Cursor.ConfigureAwait(false))
        {
            // If the pin is lost the remaining rows would be read against a floor that has already
            // moved, so the drain stops rather than finishing with a result quietly missing history.
            source.ThrowIfSnapshotLost("this materialized view refresh");

            buffer.Add(row);

            if (buffer.Count < chunkRows)
                continue;

            total += await executor.InsertRefreshChunkAsync(
                database, stagingName, source.Columns, targetColumns, buffer).ConfigureAwait(false);
            buffer.Clear();
        }

        if (buffer.Count > 0)
            total += await executor.InsertRefreshChunkAsync(
                database, stagingName, source.Columns, targetColumns, buffer).ConfigureAwait(false);

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
    private static int ResolveChunkRows(CamusDBOptions options, TableSchema view)
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
        int mutationsPerRow = 1 + (view.Indexes?.Count ?? 0);

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
    /// </remarks>
    private static async Task<bool> AcquireRefreshFenceAsync(DatabaseRegistry registry, string fenceId)
    {
        for (int attempt = 0; ; attempt++)
        {
            if (await registry.AcquireDropIntentAsync(fenceId).ConfigureAwait(false))
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
    }

    /// <summary>
    /// Derives the staging relation from the materialized view's <b>own</b> current layout rather than
    /// from the body's output schema.
    ///
    /// <para>That choice is what makes a refresh a contents-only operation. Re-deriving the shape
    /// would let a column added to a base table silently widen the materialized view — the same drift
    /// a non-materialized view's frozen column list exists to prevent — and would leave every
    /// dependent object bound to a shape that changed without a statement being issued against it.
    /// Copying the layout also copies the indexes, so the rebuilt relation is as queryable as the one
    /// it replaces the instant it is swapped in.</para>
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
            arrayElementType: column.ArrayElementType,
            defaultFunction: column.DefaultFunction,
            notNullConstraintName: column.NotNullConstraintName,
            comment: column.Comment))];

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
                : index.Type == IndexType.Unique ? ConstraintType.IndexUnique : ConstraintType.IndexMulti;

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
    /// </summary>
    private static void RequireTargetPrivilege(DatabaseDescriptor database, string viewName, TableSchema view)
    {
        if (!database.Options.AuthenticationEnabled)
            return;

        AuthorizationScope scope = AuthorizationContext.Current;

        if (scope.Principal is not null && scope.RequiredPrivilege is { } required
            && !scope.Principal.HasPrivilege(required, database.Id, view.Id))
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
