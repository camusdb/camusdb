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

/// <summary>
/// Removes one table: its row and index entries when the drop is immediate, then its schema entry
/// through the replicated <c>DropTable</c> delta.
///
/// <para><b>Nothing in memory changes before the delta is applied.</b> Every step ahead of
/// <see cref="CatalogsManager.DropTableSchema"/> is a write inside the caller's DDL transaction, which
/// rolls back if the delta is refused; the in-memory schema, the cached descriptor and the statistics
/// entry are touched only after the delta commits. The immediate path used to drop each index through
/// the <c>DROP INDEX</c> machinery, which removes the index from <c>TableSchema.Indexes</c> and from
/// the descriptor as it goes. A delta refused after that (at validation, at apply, or by a failed
/// replication) left the table in the schema with no primary key, no unique enforcement and no index
/// paths until the schema was reloaded — and a concurrent write in that window produced rows with no
/// index entries. The index entries are now purged at the key-value level only; the delta's apply
/// removes the whole <see cref="TableSchema"/>, indexes included, on every node.</para>
/// </summary>
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

    /// <summary>
    /// Drops <paramref name="table"/> inside <paramref name="tx"/>. The caller holds the DDL semaphore
    /// and has already run every rule that can refuse the drop; the delta's apply runs the rules that
    /// depend on other relations once more, in log order, and may still refuse. Up to that point this
    /// method has made no change outside the transaction.
    /// </summary>
    public async Task<bool> Drop(
        QueryExecutor queryExecutor,
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
            // Key-value entries only. The index stays in TableSchema.Indexes and in the descriptor
            // until the DropTable delta removes the table as a whole, so a refusal of that delta rolls
            // this transaction back and leaves nothing to repair. The stable index id is the Kahuna
            // key segment, so the purge reaches the physical key space even after a RENAME INDEX.
            foreach (TableIndexSchema index in table.Indexes.Values)
            {
                int purged = await table.Store.DropIndexEntries(tx, index.KvId).ConfigureAwait(false);
                Log.LogIndexEntriesPurged(logger, purged, index.Name);
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

                // Every index bucket was purged wholesale above, in this same transaction, so the
                // per-row index deletes the delete path would otherwise issue are wasted mutations.
                await rowDeleter.Delete(
                    queryExecutor, database, table, deleteTicket,
                    allowMaterializedView: true, checkForeignKeys: false, maintainIndexes: false
                ).ConfigureAwait(false);
            }
        }

        // The replicated delta. It is the last step that can refuse the drop; once it returns the
        // table is gone from every node's schema and nothing below may fail the statement.
        TableSchema? droppedSchema = await catalogs.DropTableSchema(database, ticket.TableName, tableId, tx, deferred).ConfigureAwait(false);
        Log.LogTableRemovedFromDatabaseSchema(logger, ticket.TableName);

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
