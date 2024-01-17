﻿
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
using CamusDB.Core.SQLParser;
using CamusDB.Core.Util.Time;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor;

/// <summary>
/// Facade for executing commands on the database and tables
/// </summary>
public sealed class CommandExecutor : IAsyncDisposable
{
    private readonly ILogger<ICamusDB> logger;

    private readonly HybridLogicalClock hybridLogicalClock;

    private readonly DatabaseOpener databaseOpener;

    private readonly DatabaseCreator databaseCreator;

    private readonly DatabaseCloser databaseCloser;

    private readonly DatabaseDropper databaseDroper;

    private readonly DatabaseDescriptors databaseDescriptors;

    private readonly TableOpener tableOpener;

    private readonly TableCreator tableCreator;

    private readonly TableColumnAlterer tableColumnAlterer;

    private readonly TableIndexAlterer tableIndexAlterer;

    private readonly TableDropper tableDropper;

    private readonly RowInserter rowInserter;

    private readonly RowUpdaterById rowUpdaterById;

    private readonly RowUpdater rowUpdater;

    private readonly RowDeleterById rowDeleterById;

    private readonly RowDeleter rowDeleter;

    private readonly QueryExecutor queryExecutor;

    private readonly SqlExecutor sqlExecutor;

    private readonly SchemaQuerier schemaQuerier;

    private readonly CommandValidator validator;

    /// <summary>
    /// Initializes the command executor
    /// </summary>
    /// <param name="hybridLogicalClock"></param>
    /// <param name="validator"></param>
    /// <param name="catalogs"></param>
    /// <param name="logger"></param>
    public CommandExecutor(HybridLogicalClock hybridLogicalClock, CommandValidator validator, CatalogsManager catalogs, ILogger<ICamusDB> logger)
    {       
        this.hybridLogicalClock = hybridLogicalClock;
        this.validator = validator;
        this.logger = logger;

        databaseDescriptors = new();
        databaseOpener = new(databaseDescriptors, logger);
        databaseCloser = new(databaseDescriptors, logger);
        databaseDroper = new(databaseDescriptors, logger);
        databaseCreator = new(logger);
        tableOpener = new(catalogs, logger);
        tableCreator = new(catalogs, logger);
        tableColumnAlterer = new(catalogs, logger);
        tableIndexAlterer = new(catalogs, logger);
        tableDropper = new(catalogs, logger);
        rowInserter = new(logger);
        RowUpdaterById rowUpdaterById1 = new(logger);
        rowUpdaterById = rowUpdaterById1;
        rowUpdater = new(logger);
        rowDeleterById = new(logger);
        rowDeleter = new(logger);
        queryExecutor = new(logger);
        sqlExecutor = new(logger);
        schemaQuerier = new(catalogs, logger);
    }

    #region database

    public async Task<DatabaseDescriptor> CreateDatabase(CreateDatabaseTicket ticket)
    {
        validator.Validate(ticket);

        databaseCreator.Create(ticket);

        return await databaseOpener.Open(this, hybridLogicalClock, ticket.DatabaseName).ConfigureAwait(false);
    }

    public async Task<DatabaseDescriptor> OpenDatabase(string database, bool recoveryMode = false)
    {
        return await databaseOpener.Open(this, hybridLogicalClock, database, recoveryMode).ConfigureAwait(false);
    }

    public async Task CloseDatabase(CloseDatabaseTicket ticket)
    {
        validator.Validate(ticket);

        await databaseCloser.Close(ticket.DatabaseName).ConfigureAwait(false);
    }

    public async Task DropDatabase(DropDatabaseTicket ticket)
    {
        validator.Validate(ticket);

        await databaseDroper.Drop(ticket.DatabaseName).ConfigureAwait(false);
    }

    #endregion

    #region DDL

    public async Task<bool> CreateTable(CreateTableTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(this, hybridLogicalClock, ticket.DatabaseName).ConfigureAwait(false);

        return await tableCreator.Create(queryExecutor, tableOpener, tableIndexAlterer, database, ticket).ConfigureAwait(false);
    }

    public async Task<bool> AlterTable(AlterTableTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(this, hybridLogicalClock, ticket.DatabaseName).ConfigureAwait(false);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        return await tableColumnAlterer.Alter(queryExecutor, database, table, ticket).ConfigureAwait(false);
    }

    public async Task<bool> AlterIndex(AlterIndexTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(this, hybridLogicalClock, ticket.DatabaseName).ConfigureAwait(false);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        return await tableIndexAlterer.Alter(queryExecutor, database, table, ticket).ConfigureAwait(false);
    }

    public async Task<bool> DropTable(DropTableTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(this, hybridLogicalClock, ticket.DatabaseName).ConfigureAwait(false);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        return await tableDropper.Drop(queryExecutor, tableIndexAlterer, rowDeleter, database, table, ticket).ConfigureAwait(false);
    }

    public async Task<TableDescriptor> OpenTable(OpenTableTicket ticket)
    {
        DatabaseDescriptor descriptor = await databaseOpener.Open(this, hybridLogicalClock, ticket.DatabaseName);

        return await tableOpener.Open(descriptor, ticket.TableName);
    }

    public async Task<TableDescriptor> OpenTableWithDescriptor(DatabaseDescriptor descriptor, OpenTableTicket ticket)
    {
        return await tableOpener.Open(descriptor, ticket.TableName);
    }

    public async Task<bool> ExecuteDDLSQL(ExecuteSQLTicket ticket)
    {
        validator.Validate(ticket);

        NodeAst ast = SQLParserProcessor.Parse(ticket.Sql);

        DatabaseDescriptor database = await databaseOpener.Open(this, hybridLogicalClock, ticket.DatabaseName).ConfigureAwait(false);

        switch (ast.nodeType)
        {
            case NodeType.CreateTable:
            case NodeType.CreateTableIfNotExists:
                {
                    CreateTableTicket createTableTicket = await sqlExecutor.CreateCreateTableTicket(this, ticket, ast).ConfigureAwait(false);

                    validator.Validate(createTableTicket);

                    return await tableCreator.Create(queryExecutor, tableOpener, tableIndexAlterer, database, createTableTicket);
                }

            case NodeType.AlterTableAddColumn:
            case NodeType.AlterTableDropColumn:
                {
                    AlterTableTicket alterTableTicket = sqlExecutor.CreateAlterTableTicket(await NextTxnId().ConfigureAwait(false), ticket, ast);

                    validator.Validate(alterTableTicket);

                    TableDescriptor table = await tableOpener.Open(database, alterTableTicket.TableName);

                    return await tableColumnAlterer.Alter(queryExecutor, database, table, alterTableTicket);
                }

            case NodeType.AlterTableAddIndex:
            case NodeType.AlterTableAddUniqueIndex:
            case NodeType.AlterTableDropIndex:
            case NodeType.AlterTableAddPrimaryKey:
            case NodeType.AlterTableDropPrimaryKey:
                {
                    AlterIndexTicket alterIndexTicket = sqlExecutor.CreateAlterIndexTicket(await NextTxnId(), ticket, ast);

                    validator.Validate(alterIndexTicket);

                    TableDescriptor table = await tableOpener.Open(database, alterIndexTicket.TableName);

                    return await tableIndexAlterer.Alter(queryExecutor, database, table, alterIndexTicket);
                }

            case NodeType.DropTable:
                {
                    DropTableTicket dropTableTicket = await sqlExecutor.CreateDropTableTicket(this, ticket, ast);

                    validator.Validate(dropTableTicket);

                    TableDescriptor table = await tableOpener.Open(database, dropTableTicket.TableName);

                    return await tableDropper.Drop(queryExecutor, tableIndexAlterer, rowDeleter, database, table, dropTableTicket);
                }

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Unknown DDL AST stmt: " + ast.nodeType);
        }
    }

    #endregion

    #region DML

    public async Task Insert(InsertTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(this, hybridLogicalClock, ticket.DatabaseName).ConfigureAwait(false);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        await rowInserter.Insert(database, table, ticket).ConfigureAwait(false);
    }

    public async Task InsertWithState(FluxMachine<InsertFluxSteps, InsertFluxState> machine, InsertFluxState state)
    {
        await rowInserter.InsertWithState(machine, state).ConfigureAwait(false);
    }

    /// <summary>
    /// Updates rows specifying filters and sorts
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns></returns>
    public async Task<int> Update(UpdateTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(this, hybridLogicalClock, ticket.DatabaseName).ConfigureAwait(false);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        return await rowUpdater.Update(queryExecutor, database, table, ticket).ConfigureAwait(false);
    }

    /// <summary>
    /// Updates a set of rows specifying its id
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns></returns>
    public async Task<int> UpdateById(UpdateByIdTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(this, hybridLogicalClock, ticket.DatabaseName).ConfigureAwait(false);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        return await rowUpdaterById.UpdateById(database, table, ticket).ConfigureAwait(false);
    }

    /// <summary>
    /// Deletes a row specifying its id
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns>The number of deleted rows</returns>
    public async Task<int> DeleteById(DeleteByIdTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(this, hybridLogicalClock, ticket.DatabaseName).ConfigureAwait(false);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        return await rowDeleterById.DeleteById(database, table, ticket).ConfigureAwait(false);
    }

    /// <summary>
    /// Deletes rows specifying a filter criteria
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns>The number of deleted rows</returns>
    public async Task<int> Delete(DeleteTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(this, hybridLogicalClock, ticket.DatabaseName).ConfigureAwait(false);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        return await rowDeleter.Delete(queryExecutor, database, table, ticket).ConfigureAwait(false);
    }

    /// <summary>
    /// Queries table data specifying filters and sorts
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns></returns>
    public async Task<IAsyncEnumerable<QueryResultRow>> Query(QueryTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(this, hybridLogicalClock, ticket.DatabaseName).ConfigureAwait(false);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        return queryExecutor.Query(database, table, ticket);
    }

    /// <summary>
    /// Queries a table by the row's id
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns></returns>
    public async Task<IAsyncEnumerable<Dictionary<string, ColumnValue>>> QueryById(QueryByIdTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(this, hybridLogicalClock, ticket.DatabaseName).ConfigureAwait(false);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        return queryExecutor.QueryById(database, table, ticket);
    }

    /// <summary>
    /// Execute a SQL statement that doesn't return rows
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns>The number of inserted/modified/deleted rows</returns>
    public async Task<int> ExecuteNonSQLQuery(ExecuteSQLTicket ticket)
    {
        validator.Validate(ticket);

        NodeAst ast = SQLParserProcessor.Parse(ticket.Sql);

        DatabaseDescriptor database = await databaseOpener.Open(this, hybridLogicalClock, ticket.DatabaseName).ConfigureAwait(false);

        switch (ast.nodeType)
        {
            case NodeType.Insert:
                {
                    InsertTicket updateTicket = await sqlExecutor.CreateInsertTicket(this, database, ticket, ast).ConfigureAwait(false);

                    TableDescriptor table = await tableOpener.Open(database, updateTicket.TableName).ConfigureAwait(false);

                    return await rowInserter.Insert(database, table, updateTicket).ConfigureAwait(false);
                }

            case NodeType.Update:
                {
                    UpdateTicket updateTicket = await sqlExecutor.CreateUpdateTicket(this, ticket, ast);

                    TableDescriptor table = await tableOpener.Open(database, updateTicket.TableName);

                    return await rowUpdater.Update(queryExecutor, database, table, updateTicket);
                }

            case NodeType.Delete:
                {
                    DeleteTicket deleteTicket = await sqlExecutor.CreateDeleteTicket(this, ticket, ast);

                    TableDescriptor table = await tableOpener.Open(database, deleteTicket.TableName);

                    return await rowDeleter.Delete(queryExecutor, database, table, deleteTicket);
                }

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Unknown non-query AST stmt: " + ast.nodeType);
        }
    }

    /// <summary>
    /// Execute a SQL statement that returns rows
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns></returns>
    /// <exception cref="Exception"></exception>
    public async Task<IAsyncEnumerable<QueryResultRow>> ExecuteSQLQuery(ExecuteSQLTicket ticket)
    {
        validator.Validate(ticket);

        NodeAst ast = SQLParserProcessor.Parse(ticket.Sql);

        DatabaseDescriptor database = await databaseOpener.Open(this, hybridLogicalClock, ticket.DatabaseName);

        switch (ast.nodeType)
        {
            case NodeType.Select:
                {
                    QueryTicket queryTicket = await sqlExecutor.CreateQueryTicket(this, ticket, ast);

                    TableDescriptor table = await tableOpener.Open(database, queryTicket.TableName);

                    return queryExecutor.Query(database, table, queryTicket);
                }

            case NodeType.ShowTables:
                {
                    return schemaQuerier.ShowTables(database);
                }

            case NodeType.ShowColumns:
                {
                    TableDescriptor table = await tableOpener.Open(database, ast.leftAst!.yytext!);

                    return schemaQuerier.ShowColumns(table);
                }

            case NodeType.ShowIndexes:
                {
                    TableDescriptor table = await tableOpener.Open(database, ast.leftAst!.yytext!);

                    return schemaQuerier.ShowIndexes(table);
                }

            case NodeType.ShowCreateTable:
                {
                    TableDescriptor table = await tableOpener.Open(database, ast.leftAst!.yytext!);

                    return schemaQuerier.ShowCreateTable(table);
                }

            case NodeType.ShowDatabase:
                {
                    return schemaQuerier.ShowDatabase(database);
                }

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Unknown query AST stmt: " + ast.nodeType);
        }
    }

    #endregion

    /// <summary>
    /// Generates a unique global TransactionId using the HLC
    /// </summary>
    /// <returns></returns>
    public async Task<HLCTimestamp> NextTxnId()
    {
        return await hybridLogicalClock.SendOrLocalEvent().ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        await databaseCloser.DisposeAsync();
    }
}
