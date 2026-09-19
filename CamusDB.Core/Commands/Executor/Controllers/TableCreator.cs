
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class TableCreator
{
    private readonly CatalogsManager catalogs;

    /// <summary>Configuration for this engine; injected, never ambient.</summary>
    private CamusDBOptions options;

    /// <summary>
    /// Swaps in a newly published configuration snapshot. Reference assignment is atomic and the
    /// record itself stays immutable; readers pin the field once at the top of an operation, so an
    /// in-flight operation keeps the snapshot it started with and a change takes effect at the
    /// next operation boundary.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next) => options = next;

    private readonly ILogger<ICamusDB> logger;

    public TableCreator(CatalogsManager catalogs, ILogger<ICamusDB> logger, CamusDBOptions options)
    {
        this.catalogs = catalogs;
        this.logger = logger;
        this.options = options;
    }

    public async Task<bool> Create(
        QueryExecutor queryExecutor,
        TableOpener tableOpener,
        TableIndexAlterer tableIndexAlterer,
        DatabaseDescriptor database,
        CreateTableTicket ticket,
        KvTransaction tx,
        string tableId
    )
    {
        if (ticket.IfNotExists && catalogs.TableExists(database, ticket.TableName))
            return false;

        // Every primary-key column is NOT NULL, written or not: the key is a unique index, and a
        // unique index cannot hold a row with a NULL key column (see PrimaryKeyNotNullRule).
        Controllers.DDL.PrimaryKeyNotNullRule.ApplyToCreateTable(ticket);

        int maxTables = options.MaxTablesPerDatabase;
        if (maxTables > 0 && database.Schema.Tables.Count >= maxTables)
            throw new CamusDBException(
                CamusDBErrorCodes.SchemaLimitExceeded,
                $"Database '{database.Name}' would exceed the maximum of {maxTables} tables per database");

        // Inline constraints (PRIMARY KEY / UNIQUE / INDEX) are folded into the single CreateTable
        // delta (see SchemaChangeEntryFactory.BuildInlineIndexes), so creating a table is exactly one schema
        // version and the table is born with its indexes at Public — no separate AddIndex round-trips.
        TableSchema tableSchema = await catalogs.CreateTable(database, ticket, tx, tableId).ConfigureAwait(false);

        await RegisterTableObjectAsync(database, tableSchema).ConfigureAwait(false);
        await catalogs.PersistSystemMetaAsync(database, tx).ConfigureAwait(false);

        // Build the table descriptor from the now-complete schema (table + indexes) so callers that
        // rely on TableDescriptors (statistics, query planners, key-range registration) find it
        // immediately after CreateTable.
        await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        return true;
    }

    private async Task RegisterTableObjectAsync(DatabaseDescriptor database, TableSchema tableSchema)
    {
        // Asynchronous on purpose: TableDropper holds this semaphore across an await, and a blocking
        // Wait here would park a thread until that I/O finishes (on the single-threaded browser
        // runtime there is no other thread, so the wait throws instead).
        await database.SystemSchemaSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            DatabaseTableObject tableObject = new(
                type: DatabaseObjectType.Table,
                id: tableSchema.Id ?? "",
                name: tableSchema.Name!,
                startOffset: ""
            );

            database.SystemSchema.Tables.TryAdd(tableObject.Id, tableObject);

            Log.LogTableRegisteredInSystemSpace(logger, tableSchema.Name!);
        }
        finally
        {
            database.SystemSchemaSemaphore.Release();
        }
    }
}
