
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
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

    /// <summary>
    /// Reserves the values a row insert draws for its omitted sequence-backed columns, the same way
    /// SQL <c>INSERT</c> reserves them.
    /// </summary>
    private readonly Functions.SequenceStatementBinder sequenceBinder;

    internal RowCommandService(
        ExecutorContext context,
        RowInserter rowInserter,
        RowUpdater rowUpdater,
        RowDeleter rowDeleter,
        QueryExecutor queryExecutor,
        Functions.SequenceStatementBinder sequenceBinder
    )
    {
        // Captured at construction; a binder built later in the composing constructor would be
        // captured as null and fail only when a row insert first reaches a sequence default.
        ArgumentNullException.ThrowIfNull(sequenceBinder);

        this.context = context;
        this.rowInserter = rowInserter;
        this.rowUpdater = rowUpdater;
        this.rowDeleter = rowDeleter;
        this.queryExecutor = queryExecutor;
        this.sequenceBinder = sequenceBinder;
    }

    internal async Task<InsertResult> Insert(InsertTicket ticket)
    {
        context.Validator.Validate(ticket);
        ticket.TxnState.MarkStatementExecuted();

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        // Captured before the table is opened; see WriteShapeClock for why the order matters.
        long writeShapeEpoch = database.WriteShape.Current;

        TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        SelectStatementExecutor.PinForWrite(database, table, ticket.TxnState, writeShapeEpoch);

        InsertTicket defaulted = await ApplyColumnDefaultsAsync(database, table, ticket).ConfigureAwait(false);

        int inserted = await rowInserter.Insert(database, table, defaulted).ConfigureAwait(false);
        context.Statistics.TrackInsert(database, table, inserted, defaulted.Values);
        return new(database, table, inserted);
    }

    /// <summary>
    /// Gives each row of a typed insert the defaults SQL <c>INSERT</c> gives a column its column list
    /// leaves out: a constant default, a function default evaluated per row (<c>gen_id()</c>,
    /// <c>gen_uuid_v7()</c>, <c>now()</c>), a <c>nextval</c> default, and an identity column's next
    /// value. A <c>GENERATED ALWAYS AS IDENTITY</c> column the row supplies is refused, as INSERT
    /// refuses it.
    ///
    /// <para>A row's keys are its column list, so a column is "omitted" exactly when the row has no
    /// key for it. A key present with a NULL value is a supplied NULL and is written as one, as an
    /// explicit <c>NULL</c> in a VALUES list is.</para>
    ///
    /// <para>The defaults are applied by <see cref="InsertRowShaper.ApplyOmittedDefaults"/> and the
    /// sequence values are reserved by <see cref="Functions.SequenceStatementBinder.BindAsync"/> —
    /// the code INSERT runs — so the two paths cannot disagree about what a default is. Every value
    /// a statement draws is reserved in one call per sequence before the first row is defaulted.</para>
    ///
    /// <para><b>The caller's rows are never modified.</b> Each defaulted row is a new dictionary.
    /// The transports keep the request's row and hand it to every serializable retry; a row that had
    /// absorbed the first attempt's defaults would replay them as supplied values — a stale sequence
    /// value, and a refusal for an ALWAYS identity column the caller never named.</para>
    ///
    /// <para>The new rows look keys up case-insensitively, like a row SQL INSERT builds, and carry
    /// each supplied value under the schema's spelling of its column. Two keys that name one column
    /// in different case are refused rather than resolved by whichever came last.</para>
    ///
    /// <para>Returns <paramref name="ticket"/> unchanged when the table has no column that defaults,
    /// which keeps the common case free of any copy.</para>
    /// </summary>
    private async Task<InsertTicket> ApplyColumnDefaultsAsync(DatabaseDescriptor database, TableDescriptor table, InsertTicket ticket)
    {
        List<TableColumnSchema> columns = table.Schema.Columns!;

        if (!HasDefaultingColumn(columns))
            return ticket;

        Dictionary<string, string> schemaNames = new(columns.Count, StringComparer.OrdinalIgnoreCase);
        foreach (TableColumnSchema column in columns)
            schemaNames.TryAdd(column.Name, column.Name);

        List<Dictionary<string, ColumnValue>> rows = new(ticket.Values.Count);
        InsertRowShaper[] shapers = new InsertRowShaper[ticket.Values.Count];
        List<string> sequenceDemand = [];
        bool defaultsReadStatementClock = false;

        InsertRowShaper? previous = null;

        for (int i = 0; i < ticket.Values.Count; i++)
        {
            Dictionary<string, ColumnValue> supplied = ticket.Values[i];
            Dictionary<string, ColumnValue> row = new(columns.Count, StringComparer.OrdinalIgnoreCase);
            List<string> fields = new(supplied.Count);

            foreach (KeyValuePair<string, ColumnValue> cell in supplied)
            {
                // An unknown name is kept as written; the insert validation refuses it by name.
                string name = schemaNames.TryGetValue(cell.Key, out string? schemaName) ? schemaName : cell.Key;

                if (!row.TryAdd(name, cell.Value))
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Column '{name}' specified more than once in the inserted row");

                fields.Add(name);
            }

            Functions.SequenceDefaults.RequireNoValueForAlwaysIdentity(table.Schema, fields);

            // Consecutive rows usually name the same columns, so they share one shaper.
            InsertRowShaper shaper = previous is not null && previous.Fields.SequenceEqual(fields, StringComparer.Ordinal)
                ? previous
                : InsertRowShaper.Create(table.Schema, fields);

            previous = shaper;
            shapers[i] = shaper;
            sequenceDemand.AddRange(shaper.DefaultSequenceIds);
            defaultsReadStatementClock |= shaper.DefaultsReadStatementClock;
            rows.Add(row);
        }

        ExecuteSQLTicket bound = await sequenceBinder.BindAsync(
            database,
            new ExecuteSQLTicket(ticket.TxnState, ticket.DatabaseName, sql: "", parameters: null),
            statementAst: null,
            allowedRegion: null,
            // Rows may name different columns, so the demand is listed per row instead of being one
            // per-row list multiplied by the row count: each entry is one value of that sequence.
            rowCount: 1,
            defaultSequenceIds: sequenceDemand,
            statementKind: "INSERT",
            CancellationToken.None).ConfigureAwait(false);

        // A clock default (DEFAULT now()) reads the transaction time from the parameters, so every row
        // of the batch takes one time.
        if (defaultsReadStatementClock)
            bound = Functions.ClockScalarFunctions.AttachStatementClock(bound);

        for (int i = 0; i < rows.Count; i++)
            shapers[i].ApplyOmittedDefaults(rows[i], bound.Parameters);

        Functions.SequenceStatementBinder.RecordDrawnValues(bound.Parameters, ticket.TxnState);

        return new InsertTicket(ticket.TxnState, ticket.DatabaseName, ticket.TableName, rows);
    }

    /// <summary>True when some writable column of the table gives an omitted value a default.</summary>
    private static bool HasDefaultingColumn(List<TableColumnSchema> columns)
    {
        foreach (TableColumnSchema column in columns)
        {
            if (column.DefaultValue is not null || column.DefaultFunction is not null || column.DefaultSequenceId is not null)
                return true;
        }

        return false;
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

        // Captured before the table is opened; see WriteShapeClock for why the order matters.
        long writeShapeEpoch = database.WriteShape.Current;

        TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        SelectStatementExecutor.PinForWrite(database, table, ticket.TxnState, writeShapeEpoch);

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

        // Captured before the table is opened; see WriteShapeClock for why the order matters.
        long writeShapeEpoch = database.WriteShape.Current;

        TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        SelectStatementExecutor.PinForWrite(database, table, ticket.TxnState, writeShapeEpoch);

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
