
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Flux;
using CamusDB.Core.Flux.Models;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Trees;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// 
/// </summary>
internal sealed class RowDeleter
{
    private readonly ILogger<ICamusDB> logger;

    private readonly IndexSaver indexSaver = new();

    public RowDeleter(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

    /// <summary>
    /// Schedules a new delete operation by the specified filter criteria
    /// </summary>
    /// <param name="queryExecutor"></param>
    /// <param name="database"></param>
    /// <param name="table"></param>
    /// <param name="ticket"></param>
    /// <returns>The number of deleted rows</returns>
    public async Task<int> Delete(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, DeleteTicket ticket)
    {
        DeleteFluxState state = new(
            queryExecutor: queryExecutor,
            database: database,
            table: table,
            ticket: ticket
        );

        FluxMachine<DeleteFluxSteps, DeleteFluxState> machine = new(state);

        return await DeleteInternal(machine, state).ConfigureAwait(false);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="rowValues"></param>
    /// <param name="columnNames"></param>
    /// <returns></returns>
    private static CompositeColumnValue GetColumnValue(Dictionary<string, ColumnValue> rowValues, string[] columnNames, ColumnValue? extraUniqueValue = null)
    {
        ColumnValue[] columnValues = new ColumnValue[extraUniqueValue is null ? columnNames.Length : columnNames.Length + 1];

        for (int i = 0; i < columnNames.Length; i++)
        {
            string name = columnNames[i];

            if (!rowValues.TryGetValue(name, out ColumnValue? columnValue))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A null value was found for unique key field '" + name + "'"
                );

            columnValues[i] = columnValue;
        }

        if (extraUniqueValue is not null)
            columnValues[^1] = extraUniqueValue;

        return new CompositeColumnValue(columnValues);
    }

    /// <summary>
    /// We need to locate the row tuples to delete
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> LocateTupleToDelete(DeleteFluxState state)
    {
        DeleteTicket ticket = state.Ticket;

        QueryTicket queryTicket = new(
            txnState: ticket.TxnState,
            txnType: TransactionType.Write,
            databaseName: ticket.DatabaseName,
            tableName: ticket.TableName,
            index: null,
            projection: null,
            filters: ticket.Filters,
            where: ticket.Where,
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null
        );

        IAsyncEnumerable<QueryResultRow> cursor = state.QueryExecutor.Query(state.Database, state.Table, queryTicket);

        // @todo we need to take a snapshot of the data to prevent deadlocks
        // but probably need to optimize this for larger datasets
        state.RowsToDelete = await cursor.ToListAsync().ConfigureAwait(false);

        return FluxAction.Continue;
    }

    /// <summary>
    /// Acquire write locks on the indices to ensure consistency in writing.
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>    
    private async Task<FluxAction> TryAdquireLocks(DeleteFluxState state)
    {
        await state.Ticket.TxnState.TryAdquireWriteLocks(state.Table).ConfigureAwait(false);

        return FluxAction.Continue;
    }

    /// <summary>
    /// Deletes the row from disk
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> DeleteRowsAndIndexesFromDisk(DeleteFluxState state)
    {
        if (state.RowsToDelete is null || state.RowsToDelete.Count == 0)
        {
            logger.LogError("Invalid rows to delete");

            return FluxAction.Abort;
        }

        DeleteTicket ticket = state.Ticket;
        TableDescriptor table = state.Table;
        ObjectIdValue nullPageOffset = new();

        foreach (QueryResultRow row in state.RowsToDelete)
        {
            BTreeTuple tuple = new(row.Tuple.SlotOne, nullPageOffset);

            await DeleteFromTableIndex(state, tuple).ConfigureAwait(false);

            await UpdateUniqueIndexes(state, table, ticket, tuple, row).ConfigureAwait(false);

            await UpdateMultiIndexes(state, table, ticket, tuple, row).ConfigureAwait(false);

            logger.LogInformation(
                "Row with rowid {SlotOne} deleted to tombstone page {SlotTwo}",
                tuple.SlotOne,
                tuple.SlotTwo
            );

            state.DeletedRows++;
        }

        return FluxAction.Continue;
    }

    private async Task DeleteFromTableIndex(DeleteFluxState state, BTreeTuple tuple)
    {
        SaveOffsetIndexTicket saveUniqueOffsetIndex = new(
            tablespace: state.Database.BufferPool,
            index: state.Table.Rows,
            txnId: state.Ticket.TxnState.TxnId,
            commitState: BTreeCommitState.Uncommitted,
            key: tuple.SlotOne,
            value: tuple.SlotTwo,
            modifiedPages: state.Ticket.TxnState.ModifiedPages
        );

        // Main table index stores rowid pointing to page offset
        await indexSaver.Save(saveUniqueOffsetIndex).ConfigureAwait(false);

        state.Ticket.TxnState.MainTableDeltas.Add((state.Table.Rows, tuple));
    }

    private async Task UpdateUniqueIndexes(
        DeleteFluxState state,
        TableDescriptor table,
        DeleteTicket ticket,
        BTreeTuple tuple,
        QueryResultRow row
    )
    {
        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            TableIndexSchema index = kv.Value;

            if (index.Type != IndexType.Unique)
                continue;

            BTree<CompositeColumnValue, BTreeTuple> uniqueIndex = index.BTree;

            CompositeColumnValue? uniqueKeyValue = GetColumnValue(row.Row, index.Columns);

            SaveIndexTicket saveUniqueIndexTicket = new(
                tablespace: state.Database.BufferPool,
                index: uniqueIndex,
                txnId: ticket.TxnState.TxnId,
                commitState: BTreeCommitState.Uncommitted,
                key: uniqueKeyValue,
                value: tuple,
                modifiedPages: state.Ticket.TxnState.ModifiedPages
            );

            //Console.WriteLine("Saving unique index {0} {1} {2}", uniqueIndex, uniqueKeyValue, tuple);

            await indexSaver.Save(saveUniqueIndexTicket).ConfigureAwait(false);

            state.Ticket.TxnState.UniqueIndexDeltas.Add((uniqueIndex, uniqueKeyValue, tuple));
        }
    }

    private async Task UpdateMultiIndexes(
        DeleteFluxState state,
        TableDescriptor table,
        DeleteTicket ticket,
        BTreeTuple tuple,
        QueryResultRow row
    )
    {
        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            TableIndexSchema index = kv.Value;

            if (index.Type != IndexType.Multi)
                continue;

            BTree<CompositeColumnValue, BTreeTuple> multiIndex = index.BTree;

            CompositeColumnValue multiKeyValue = GetColumnValue(row.Row, index.Columns, new ColumnValue(ColumnType.Id, tuple.SlotOne.ToString()));

            SaveIndexTicket saveUniqueIndexTicket = new(
                tablespace: state.Database.BufferPool,
                index: multiIndex,
                txnId: ticket.TxnState.TxnId,
                commitState: BTreeCommitState.Uncommitted,
                key: multiKeyValue,
                value: tuple,
                modifiedPages: state.Ticket.TxnState.ModifiedPages
            );

            //Console.WriteLine("Saving unique index {0} {1} {2}", uniqueIndex, uniqueKeyValue, tuple);

            await indexSaver.Save(saveUniqueIndexTicket).ConfigureAwait(false);

            state.Ticket.TxnState.MultiIndexDeltas.Add((multiIndex, multiKeyValue, tuple));
        }
    }

    /// <summary>
    /// Executes the flux state machine to delete a set of records that match the specified criteria
    /// </summary>
    /// <param name="machine"></param>
    /// <param name="state"></param>
    /// <returns></returns>
    public async Task<int> DeleteInternal(FluxMachine<DeleteFluxSteps, DeleteFluxState> machine, DeleteFluxState state)
    {
        Stopwatch timer = Stopwatch.StartNew();

        machine.When(DeleteFluxSteps.TryAdquireLocks, TryAdquireLocks);
        machine.When(DeleteFluxSteps.LocateTupleToDelete, LocateTupleToDelete);
        machine.When(DeleteFluxSteps.DeleteRowsAndIndexesFromDisk, DeleteRowsAndIndexesFromDisk);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep()).ConfigureAwait(false);

        timer.Stop();

        TimeSpan timeTaken = timer.Elapsed;

        logger.LogInformation(
            "Deleted {Rows} rows, Time taken: {Time}",
            state.DeletedRows,
            timeTaken.ToString(@"m\:ss\.fff")
        );

        return state.DeletedRows;
    }
}
