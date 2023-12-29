
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class TableAlterer
{
    private CatalogsManager Catalogs { get; set; }

    private readonly TableColumnAdder tableColumnAdder = new();

    private readonly TableColumnDropper tableColumnDropper = new();

    public TableAlterer(CatalogsManager catalogsManager)
    {
        Catalogs = catalogsManager;
    }

    public async Task<bool> Alter(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, AlterTableTicket ticket)
    {
        TableSchema newTableSchema = await Catalogs.AlterTable(database, ticket);

        return ticket.Operation switch
        {
            AlterTableOperation.AddColumn => await AddColumn(queryExecutor, database, table, ticket),
            AlterTableOperation.DropColumn => await DropColumn(queryExecutor, database, table, ticket),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid alter table operation"),
        };
    }

    private async Task<bool> AddColumn(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, AlterTableTicket ticket)
    {
        AlterColumnTicket alterColumnTicket = new(database.Name, table.Name, ticket.Column.Name);

        await tableColumnAdder.AddColumn(queryExecutor, database, table, alterColumnTicket);

        return true;
    }

    private async Task<bool> DropColumn(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, AlterTableTicket ticket)
    {
        AlterColumnTicket alterColumnTicket = new(database.Name, table.Name, ticket.Column.Name);

        await tableColumnDropper.DropColumn(queryExecutor, database, table, alterColumnTicket);

        return true;
    }
}

