
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class TableIndexAlterer
{
    private readonly CatalogsManager catalogs;

    private readonly TableIndexAdder tableIndexAdder;

    public TableIndexAlterer(CatalogsManager catalogsManager, ILogger<ICamusDB> logger)
    {
        catalogs = catalogsManager;

        tableIndexAdder = new(logger);
    }

    public async Task<bool> Alter(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, AlterIndexTicket ticket, KvTransaction tx)
    {
        return ticket.Operation switch
        {
            AlterIndexOperation.AddIndex or AlterIndexOperation.AddUniqueIndex or AlterIndexOperation.AddPrimaryKey or AlterIndexOperation.AddInvertedIndex
                => await AddIndex(catalogs, queryExecutor, database, table, ticket, tx).ConfigureAwait(false),

            // A drop has no local work: outside a transaction the coordinator steps the index down on every
            // node (SchemaDdlService.DropIndexStagedWithinGateAsync), and inside one it leaves the private
            // schema only.
            AlterIndexOperation.DropIndex or AlterIndexOperation.DropPrimaryKey =>
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Index '{ticket.IndexName}' of table '{table.Name}' is dropped by the staged coordinator, not by local work"),

            AlterIndexOperation.RenameIndex
                => await RenameIndex(database, ticket, tx).ConfigureAwait(false),

            _ =>
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid alter index operation"),
        };
    }

    private async Task<bool> AddIndex(CatalogsManager catalogs, QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, AlterIndexTicket ticket, KvTransaction tx)
    {
        if (ticket.IfNotExists && table.AllIndexes.ContainsKey(ticket.IndexName))
            return false;

        await tableIndexAdder.AddIndex(catalogs, tx, queryExecutor, database, table, ticket).ConfigureAwait(false);
        return true;
    }

    private async Task<bool> RenameIndex(DatabaseDescriptor database, AlterIndexTicket ticket, KvTransaction tx)
    {
        await catalogs.RenameIndexInTableAsync(database, ticket, tx).ConfigureAwait(false);
        return true;
    }
}
