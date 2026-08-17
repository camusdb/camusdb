
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

/// <summary>
/// The ticket-based row operations — insert, update, delete, scan, and point read — for callers that
/// build a typed ticket instead of sending SQL.
///
/// <para>Every one follows the same four steps in the same order, and the order is what keeps them
/// correct: validate the ticket, mark the transaction as having executed a statement, resolve the
/// database and table, then <b>pin the table's schema version into the transaction</b> before any
/// row is touched. The pin is what makes a statement read and write one schema: without it a
/// concurrent DDL could change the layout underneath a multi-step operation, and rows written before
/// and after the change would disagree about what they mean.</para>
///
/// <para>The two read paths additionally refuse a materialized view that is mid-refresh, because its
/// contents are a partially rebuilt relation rather than a queryable snapshot.</para>
///
/// <para><c>QueryById</c> is deliberately the one operation that does <em>not</em> mark a statement
/// executed: it is a point read used by transaction plumbing, not a user statement.</para>
/// </summary>
internal sealed class RowCommandService
{
    private readonly ExecutorContext context;

    private readonly RowInserter rowInserter;

    private readonly RowUpdater rowUpdater;

    private readonly RowDeleter rowDeleter;

    private readonly QueryExecutor queryExecutor;

    internal RowCommandService(
        ExecutorContext context,
        RowInserter rowInserter,
        RowUpdater rowUpdater,
        RowDeleter rowDeleter,
        QueryExecutor queryExecutor
    )
    {
        this.context = context;
        this.rowInserter = rowInserter;
        this.rowUpdater = rowUpdater;
        this.rowDeleter = rowDeleter;
        this.queryExecutor = queryExecutor;
    }

    internal async Task<InsertResult> Insert(InsertTicket ticket)
    {
        context.Validator.Validate(ticket);
        ticket.TxnState.MarkStatementExecuted();

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        SelectStatementExecutor.PinSchemaVersion(database, table, ticket.TxnState);

        int inserted = await rowInserter.Insert(database, table, ticket).ConfigureAwait(false);
        context.Statistics.TrackInsert(database, table, inserted, ticket.Values);
        return new(database, table, inserted);
    }

    /// <summary>
    /// Updates rows specifying filters and sorts
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns></returns>
    internal async Task<UpdateResult> Update(UpdateTicket ticket)
    {
        context.Validator.Validate(ticket);
        ticket.TxnState.MarkStatementExecuted();

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        SelectStatementExecutor.PinSchemaVersion(database, table, ticket.TxnState);

        int updated = await rowUpdater.Update(queryExecutor, database, table, ticket).ConfigureAwait(false);
        context.Statistics.TrackUpdate(database, table, updated, ticket.PlainValues);
        return new(database, table, updated);
    }

    /// <summary>
    /// Deletes rows specifying a filter criteria
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns>The number of deleted rows</returns>
    internal async Task<DeleteResult> Delete(DeleteTicket ticket)
    {
        context.Validator.Validate(ticket);
        ticket.TxnState.MarkStatementExecuted();

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        SelectStatementExecutor.PinSchemaVersion(database, table, ticket.TxnState);

        int deleted = await rowDeleter.Delete(queryExecutor, database, table, ticket).ConfigureAwait(false);
        context.Statistics.TrackDelete(database, table, deleted);
        return new(database, table, deleted);
    }

    /// <summary>
    /// Queries table data specifying filters and sorts
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns></returns>
    internal async Task<(DatabaseDescriptor, IAsyncEnumerable<QueryResultRow>)> Query(QueryTicket ticket)
    {
        context.Validator.Validate(ticket);
        ticket.TxnState.MarkStatementExecuted();

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);

        TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        MaterializedViewAccessGuard.RequireReadable(table);
        SelectStatementExecutor.PinSchemaVersion(database, table, ticket.TxnState);

        return (database, queryExecutor.Query(database, table, ticket));
    }

    /// <summary>
    /// Queries a table by the row's id
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns></returns>
    internal async Task<IAsyncEnumerable<Dictionary<string, ColumnValue>>> QueryById(QueryByIdTicket ticket)
    {
        context.Validator.Validate(ticket);

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);

        TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        MaterializedViewAccessGuard.RequireReadable(table);
        SelectStatementExecutor.PinSchemaVersion(database, table, ticket.TxnState);

        return queryExecutor.QueryById(database, table, ticket);
    }}
