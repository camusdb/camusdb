
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor;

public sealed class CommandExecutor
{
    private readonly DatabaseOpener databaseOpener = new();

    private readonly DatabaseCreator databaseCreator = new();

    private readonly TableOpener tableOpener;

    private readonly TableCreator tableCreator;

    private readonly RowInserter rowInserter = new();

    private readonly QueryExecutor queryExecutor = new();

    public CommandExecutor(CatalogsManager catalogsManager)
    {
        tableCreator = new(catalogsManager);
        tableOpener = new(catalogsManager);
    }

    public async Task<bool> CreateTable(CreateTableTicket ticket)
    {
        DatabaseDescriptor descriptor = await databaseOpener.Open(ticket.Database);
        return await tableCreator.Create(descriptor, ticket);
    }

    public async Task CreateDatabase(string name)
    {
        await databaseCreator.Create(name);
    }

    public async Task<DatabaseDescriptor> OpenDatabase(string database)
    {
        return await databaseOpener.Open(database);        
    }

    public async Task Insert(InsertTicket ticket)
    {
        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName);        

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName);

        await rowInserter.Insert(database, table, ticket);
    }

    public async Task<List<List<ColumnValue>>> Query(QueryTicket ticket)
    {
        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName);        

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName);

        return await queryExecutor.Query(database, table, ticket);
    }

    public async Task<List<List<ColumnValue>>> QueryById(QueryByIdTicket ticket)
    {
        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName);

        return await queryExecutor.QueryById(database, table, ticket);
    }
}
