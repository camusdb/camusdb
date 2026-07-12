
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

    private readonly ILogger<ICamusDB> logger;

    public TableCreator(CatalogsManager catalogs, ILogger<ICamusDB> logger)
    {
        this.catalogs = catalogs;
        this.logger = logger;
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

        int maxTables = CamusDBConfig.MaxTablesPerDatabase;
        if (maxTables > 0 && database.Schema.Tables.Count >= maxTables)
            throw new CamusDBException(
                CamusDBErrorCodes.SchemaLimitExceeded,
                $"Database '{database.Name}' would exceed the maximum of {maxTables} tables per database");

        // Inline constraints (PRIMARY KEY / UNIQUE / INDEX) are folded into the single CreateTable
        // delta (see CatalogsManager.BuildInlineIndexes), so creating a table is exactly one schema
        // version and the table is born with its indexes at Public — no separate AddIndex round-trips.
        TableSchema tableSchema = await catalogs.CreateTable(database, ticket, tx, tableId).ConfigureAwait(false);

        RegisterTableObject(database, tableSchema);
        await catalogs.PersistSystemMetaAsync(database, tx).ConfigureAwait(false);

        // Build the table descriptor from the now-complete schema (table + indexes) so callers that
        // rely on TableDescriptors (statistics, query planners, key-range registration) find it
        // immediately after CreateTable.
        await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        return true;
    }

    private void RegisterTableObject(DatabaseDescriptor database, TableSchema tableSchema)
    {
        try
        {
            database.SystemSchemaSemaphore.Wait();

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
