
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Flux;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;

namespace CamusDB.Core.CommandsExecutor;

public sealed class CommandExecutor : IDisposable
{
    private readonly DatabaseOpener databaseOpener;

    private readonly DatabaseCreator databaseCreator;

    private readonly DatabaseCloser databaseCloser;

    private readonly DatabaseDescriptors databaseDescriptors;

    private readonly TableOpener tableOpener;

    private readonly TableCreator tableCreator;

    private readonly RowInserter rowInserter;

    private readonly RowDeleter rowDeleter;

    private readonly QueryExecutor queryExecutor;

    private readonly CommandValidator validator;

    public CommandExecutor(CommandValidator validator, CatalogsManager catalogs)
    {
        this.validator = validator;

        databaseDescriptors = new();
        databaseOpener = new(databaseDescriptors);
        databaseCloser = new(databaseDescriptors);
        databaseCreator = new();
        tableOpener = new(catalogs);
        tableCreator = new(catalogs);
        rowInserter = new();
        rowDeleter = new();
        queryExecutor = new();
    }

    #region database

    public async Task CreateDatabase(CreateDatabaseTicket ticket)
    {
        validator.Validate(ticket);

        await databaseCreator.Create(ticket);
    }

    public async Task<DatabaseDescriptor> OpenDatabase(string database, bool recoveryMode = false)
    {
        return await databaseOpener.Open(this, database, recoveryMode);
    }

    public async Task CloseDatabase(CloseDatabaseTicket ticket)
    {
        validator.Validate(ticket);

        await databaseCloser.Close(ticket.DatabaseName);
    }

    #endregion

    #region DDL

    public async Task<bool> CreateTable(CreateTableTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor descriptor = await databaseOpener.Open(this, ticket.DatabaseName);
        return await tableCreator.Create(descriptor, ticket);
    }

    public async Task<TableDescriptor> OpenTable(OpenTableTicket ticket)
    {
        DatabaseDescriptor descriptor = await databaseOpener.Open(this, ticket.DatabaseName);
        return await tableOpener.Open(descriptor, ticket.TableName);
    }

    public async Task<TableDescriptor> OpenTableWithDescriptor(DatabaseDescriptor descriptor, OpenTableTicket ticket)
    {
        return await tableOpener.Open(descriptor, ticket.TableName);
    }

    #endregion

    #region DML

    public async Task Insert(InsertTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(this, ticket.DatabaseName);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName);

        await rowInserter.Insert(database, table, ticket);
    }
    
    public async Task InsertWithState(FluxMachine<InsertFluxSteps, InsertFluxState> machine, InsertFluxState state)
    {
        await rowInserter.InsertWithState(machine, state);
    }

    public async Task DeleteById(DeleteByIdTicket ticket)
    {
        //validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(this, ticket.DatabaseName);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName);

        await rowDeleter.DeleteById(database, table, ticket);
    }

    public async Task<List<Dictionary<string, ColumnValue>>> Query(QueryTicket ticket)
    {
        DatabaseDescriptor database = await databaseOpener.Open(this, ticket.DatabaseName);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName);

        return await queryExecutor.Query(database, table, ticket);
    }

    public async Task<List<Dictionary<string, ColumnValue>>> QueryById(QueryByIdTicket ticket)
    {
        DatabaseDescriptor database = await databaseOpener.Open(this, ticket.DatabaseName);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName);

        return await queryExecutor.QueryById(database, table, ticket);
    }

    #endregion    

    public void Dispose()
    {
        databaseCloser.Dispose();
    }
}
