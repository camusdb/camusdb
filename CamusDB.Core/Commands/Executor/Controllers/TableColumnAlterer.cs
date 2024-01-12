
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

internal sealed class TableColumnAlterer
{
    private readonly CatalogsManager catalogs;

    private readonly TableColumnAdder tableColumnAdder = new();

    private readonly TableColumnDropper tableColumnDropper = new();

    public TableColumnAlterer(CatalogsManager catalogsManager)
    {
        catalogs = catalogsManager;
    }

    public async Task<bool> Alter(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, AlterTableTicket ticket)
    {        
        return ticket.Operation switch
        {
            AlterTableOperation.AddColumn => await AddColumn(queryExecutor, database, table, ticket),
            AlterTableOperation.DropColumn => await DropColumn(queryExecutor, database, table, ticket),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid alter table operation"),
        };
    }    

    private async Task<bool> AddColumn(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, AlterTableTicket ticket)
    {
        AlterColumnTicket alterColumnTicket = new(
            txnId: ticket.TxnId,
            databaseName: database.Name,
            tableName: table.Name,
            column: ticket.Column,
            operation: ticket.Operation
        );

        await tableColumnAdder.AddColumn(catalogs, queryExecutor, database, table, alterColumnTicket);

        return true;
    }

    private async Task<bool> DropColumn(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, AlterTableTicket ticket)
    {
        AlterColumnTicket alterColumnTicket = new(
            txnId: ticket.TxnId,
            databaseName: database.Name,
            tableName: table.Name,
            column: ticket.Column,
            operation: ticket.Operation
        );

        await tableColumnDropper.DropColumn(catalogs, queryExecutor, database, table, alterColumnTicket);

        return true;
    }
}

