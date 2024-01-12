
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

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class TableIndexAlterer
{
    private readonly CatalogsManager catalogs;

    private readonly TableIndexAdder tableIndexAdder = new();

    private readonly TableIndexDropper tableIndexDropper = new();

    public TableIndexAlterer(CatalogsManager catalogsManager)
    {
        catalogs = catalogsManager;
    }

    public async Task<bool> Alter(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, AlterIndexTicket ticket)
    {
        return ticket.Operation switch
        {
            AlterIndexOperation.AddIndex => await AddIndex(queryExecutor, database, table, ticket),
            AlterIndexOperation.DropIndex => await DropIndex(queryExecutor, database, table, ticket),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid alter table operation"),
        };
    }

    private async Task<bool> AddIndex(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, AlterIndexTicket ticket)
    {        
        await tableIndexAdder.AddIndex(queryExecutor, database, table, ticket);
        return true;
    }

    private async Task<bool> DropIndex(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, AlterIndexTicket ticket)
    {
        await tableIndexDropper.DropIndex(queryExecutor, database, table, ticket);
        return true;
    }
}