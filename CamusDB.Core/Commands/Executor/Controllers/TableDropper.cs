
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Statistics;
using CamusDB.Core.Transactions;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class TableDropper
{
    private readonly CatalogsManager catalogs;

    private readonly StatisticsManager statistics;

    private readonly ILogger<ICamusDB> logger;

    public TableDropper(CatalogsManager catalogs, StatisticsManager statistics, ILogger<ICamusDB> logger)
    {
        this.catalogs = catalogs;
        this.statistics = statistics;
        this.logger = logger;
    }

    public async Task<bool> Drop(
        QueryExecutor queryExecutor,
        TableIndexAlterer tableIndexAlterer,
        RowDeleter rowDeleter,
        DatabaseDescriptor database,
        TableDescriptor table,
        DropTableTicket ticket,
        KvTransaction tx
    )
    {
        string tableId = table.Id;

        // A table in a root database dropped without FORCE is retained as a recoverable orphan: its
        // rows, index entries, and schema-history keys are left on disk and the table id becomes
        // relinkable until the garbage collector reclaims it after the retention window. Tables in
        // branch databases (and any FORCE drop) take the immediate path — their rows live in ancestor
        // COW overlays whose recovery is out of scope.
        bool deferred = !ticket.Force && database.Ancestors.Count == 0;

        if (deferred)
        {
            // Retention only: indexes and rows are intentionally NOT dropped so their KV data survives
            // for recovery. The orphan record is written by the replicated drop's checkpoint (in the
            // same transaction that deletes the per-table meta key), not here — see
            // CatalogsManager.PersistDroppedTableAsync — so the detach and the recovery record commit
            // atomically even if this outer DDL transaction later fails.
        }
        else
        {
            foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
            {
                AlterIndexTicket alterIndexTicket = new(
                    databaseName: ticket.DatabaseName,
                    tableName: ticket.TableName,
                    indexName: index.Key,
                    columns: Array.Empty<ColumnIndexInfo>(),
                    operation: index.Key == CamusDBConstants.PrimaryKeyInternalName ? AlterIndexOperation.DropPrimaryKey : AlterIndexOperation.DropIndex
                );

                await tableIndexAlterer.Alter(queryExecutor, database, table, alterIndexTicket, tx).ConfigureAwait(false);
            }

            // On a branch database the table's inherited rows live in ancestor keyspaces and are already
            // unreachable once the schema no longer references this table — no tombstones are needed.
            // Only the branch-local overlay entries (if any) need to be physically removed.
            // On a root database every row is in the local keyspace and must be logically deleted so
            // active transactions see a correct row count and the DML delete path fires correctly.
            if (database.Ancestors.Count > 0)
            {
                await table.Store.PurgeLocalRowOverlayAsync(tx).ConfigureAwait(false);
            }
            else
            {
                DeleteTicket deleteTicket = new(
                    txnState: tx,
                    databaseName: ticket.DatabaseName,
                    tableName: ticket.TableName,
                    where: null,
                    filters: null
                );

                await rowDeleter.Delete(queryExecutor, database, table, deleteTicket, allowMaterializedView: true).ConfigureAwait(false);
            }
        }

        // Statistics cleanup. Both paths evict the in-memory entry (otherwise it leaks for the
        // process lifetime, and a pending background flush could re-create the persisted blob
        // after the drop). The deferred path intentionally keeps the persisted blob: a RELINK
        // restores the table and reloads it, and the orphan reclaimer purges it otherwise. The
        // immediate path deletes the blob inside this DDL transaction — nothing else ever would.
        if (deferred)
        {
            statistics.EvictTableStats(database, tableId);
        }
        else
        {
            try
            {
                await statistics.DropTableStatsAsync(database, tableId, tx).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Best-effort: stats are advisory and table ids are never reused, so a leftover
                // blob is harmless garbage removed by DROP DATABASE; don't fail the drop over it.
                logger.LogWarning(ex, "Failed to delete stats blob for dropped table '{TableName}'", ticket.TableName);
            }
        }

        TableSchema? droppedSchema = await catalogs.DropTableSchema(database, ticket.TableName, tableId, tx, deferred).ConfigureAwait(false);
        Log.LogTableRemovedFromDatabaseSchema(logger, ticket.TableName);

        // In cluster mode delete all persisted coordinator job records for this table so a
        // subsequent leader-change resume cannot replay them against a new table that happens
        // to share the same name.
        //
        // Ordering: DROP TABLE is a replicated schema op serialized through the schema lock and
        // the single-schema-leader invariant. Any in-progress coordinator step for this table is
        // also serialized through that path, so the coordinator cannot be mid-step when we reach
        // here. We rely on that serialization rather than explicitly cancelling the in-memory job.
        //
        // Best-effort: a transient KV failure is logged and swallowed — a failed delete leaves an
        // orphan that will fail harmlessly on its first resume step because the table schema is
        // already gone. The resume-time table-id mismatch check is the structural backstop for
        // the aliasing hazard.
        try
        {
            await catalogs.DeleteCoordinatorJobsForTableAsync(database, tableId, tx).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to clean up coordinator jobs for dropped table '{TableName}'", ticket.TableName);
        }

        try
        {
            await database.SystemSchemaSemaphore.WaitAsync().ConfigureAwait(false);

            if (database.SystemSchema.Tables.Remove(table.Id))
                Log.LogTableRemovedFromSystemSchema(logger, ticket.TableName);

            await catalogs.PersistMetaAsync(database, tx).ConfigureAwait(false);
        }
        finally
        {
            database.SystemSchemaSemaphore.Release();
        }

        database.TableDescriptors.TryRemove(ticket.TableName, out _);

        Log.LogTableDropped(logger, ticket.TableName);

        return true;
    }
}
