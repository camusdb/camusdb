
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor;

public sealed class CommandExecutor
{
    private readonly DatabaseOpener databaseOpener;

    private readonly DatabaseCreator databaseCreator;

    private readonly TableOpener tableOpener;

    private readonly TableCreator tableCreator;

    private readonly RowInserter rowInserter;

    private readonly QueryExecutor queryExecutor;

    private readonly CommandValidator validator;

    public CommandExecutor(CommandValidator validator, CatalogsManager catalogs)
    {
        this.validator = validator;

        databaseOpener = new();
        databaseCreator = new();
        tableOpener = new(catalogs);
        tableCreator = new(catalogs);
        rowInserter = new();
        queryExecutor = new(validator);
    }

    public async Task<bool> CreateTable(CreateTableTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor descriptor = await databaseOpener.Open(ticket.DatabaseName);
        return await tableCreator.Create(descriptor, ticket);
    }

    public async Task CreateDatabase(CreateDatabaseTicket ticket)
    {
        validator.Validate(ticket);

        await databaseCreator.Create(ticket);
    }

    public async Task<DatabaseDescriptor> OpenDatabase(string database)
    {
        return await databaseOpener.Open(database);        
    }

    public async Task Insert(InsertTicket ticket)
    {
        validator.Validate(ticket);

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
