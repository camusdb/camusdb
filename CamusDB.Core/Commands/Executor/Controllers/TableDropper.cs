
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
using CamusDB.Core.Transactions;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class TableDropper
{
    private readonly CatalogsManager catalogs;

    private readonly ILogger<ICamusDB> logger;

    public TableDropper(CatalogsManager catalogs, ILogger<ICamusDB> logger)
    {
        this.catalogs = catalogs;
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
        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            AlterIndexTicket alterIndexTicket = new(
                databaseName: ticket.DatabaseName,
                tableName: ticket.TableName,
                indexName: index.Key,
                columns: Array.Empty<ColumnIndexInfo>(),
                operation: index.Key == CamusDBConfig.PrimaryKeyInternalName ? AlterIndexOperation.DropPrimaryKey : AlterIndexOperation.DropIndex
            );

            await tableIndexAlterer.Alter(queryExecutor, database, table, alterIndexTicket, tx).ConfigureAwait(false);
        }

        DeleteTicket deleteTicket = new(
            txnState: tx,
            databaseName: ticket.DatabaseName,
            tableName: ticket.TableName,
            where: null,
            filters: null
        );

        await rowDeleter.Delete(queryExecutor, database, table, deleteTicket).ConfigureAwait(false);

        string tableId = table.Id;

        TableSchema? droppedSchema = await catalogs.DropTableSchema(database, ticket.TableName, tableId, tx).ConfigureAwait(false);
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
            await catalogs.DeleteCoordinatorJobsForTableAsync(database, tableId).ConfigureAwait(false);
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
