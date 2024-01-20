
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
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class TableIndexAlterer
{
    private readonly CatalogsManager catalogs;

    private readonly TableIndexAdder tableIndexAdder;

    private readonly TableIndexDropper tableIndexDropper;

    public TableIndexAlterer(CatalogsManager catalogsManager, ILogger<ICamusDB> logger)
    {
        catalogs = catalogsManager;

        tableIndexAdder = new(logger);
        tableIndexDropper = new(logger);
    }

    public async Task<bool> Alter(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, AlterIndexTicket ticket)
    {
        return ticket.Operation switch
        {
            AlterIndexOperation.AddIndex or AlterIndexOperation.AddUniqueIndex or AlterIndexOperation.AddPrimaryKey 
                => await AddIndex(catalogs, queryExecutor, database, table, ticket),
                
            AlterIndexOperation.DropIndex or AlterIndexOperation.DropPrimaryKey
                => await DropIndex(catalogs, queryExecutor, database, table, ticket),

            _ => 
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid alter table operation"),
        };
    }

    private async Task<bool> AddIndex(CatalogsManager catalogs, QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, AlterIndexTicket ticket)
    {        
        await tableIndexAdder.AddIndex(catalogs, queryExecutor, database, table, ticket);
        return true;
    }

    private async Task<bool> DropIndex(CatalogsManager catalogs, QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, AlterIndexTicket ticket)
    {
        await tableIndexDropper.DropIndex(queryExecutor, database, table, ticket);
        return true;
    }
}