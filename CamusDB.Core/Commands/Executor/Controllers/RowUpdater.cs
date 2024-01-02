
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
using CamusDB.Core.Util.Time;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Updates multiple rows by the specified filters
/// </summary>
public sealed class RowUpdater
{
    private readonly IndexSaver indexSaver = new();

    private readonly RowSerializer rowSerializer = new();    

    /// <summary>
    /// Validates that all columns and values in the update statement are valid
    /// </summary>
    /// <param name="table"></param>
    /// <param name="ticket"></param>
    /// <exception cref="CamusDBException"></exception>
    private static void Validate(TableDescriptor table, UpdateTicket ticket) // @todo optimize this
    {
        List<TableColumnSchema> columns = table.Schema.Columns!;

        if (ticket.Values is null || ticket.Values.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing columns list to update");

        foreach (KeyValuePair<string, ColumnValue> columnValue in ticket.Values)
        {
            bool hasColumn = false;

            for (int i = 0; i < columns.Count; i++)
            {
                TableColumnSchema column = columns[i];

                if (string.IsNullOrEmpty(columnValue.Key))
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Invalid or empty column name in values list");

                if (string.Equals(column.Name, columnValue.Key))
                {
                    if (column.Primary)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Cannot update primary key field");

                    hasColumn = true;
                    break;
                }
            }

            if (!hasColumn)
                throw new CamusDBException(
                    CamusDBErrorCodes.UnknownColumn,
                    $"Unknown column '{columnValue.Key}' in column list"
                );
        }

        foreach (TableColumnSchema columnSchema in columns)
        {
            if (!columnSchema.NotNull)
                continue;

            if (!ticket.Values.TryGetValue(columnSchema.Name, out ColumnValue? columnValue))
                continue;

            if (columnValue.Type == ColumnType.Null)
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.NotNullViolation,
                    $"Column '{columnSchema.Name}' cannot be null"
                );
            }
        }
    }

    /// <summary>
    /// Schedules a new Update operation by the specified filters
    /// </summary>
    /// <param name="database"></param>
    /// <param name="table"></param>
    /// <param name="ticket"></param>
    /// <returns></returns>
    internal async Task<int> Update(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, UpdateTicket ticket)
    {
        Validate(table, ticket);

        UpdateFluxState state = new(
            database: database,
            table: table,
            ticket: ticket,
            queryExecutor: queryExecutor,
            indexes: GetIndexUpdatePlan(table, ticket)
        );

        FluxMachine<UpdateFluxSteps, UpdateFluxState> machine = new(state);

        return await UpdateInternal(machine, state);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="columnValues"></param>
    /// <param name="columnName"></param>
    /// <returns></returns>
    private static ColumnValue? GetColumnValue(Dictionary<string, ColumnValue> columnValues, string columnName)
    {
        if (columnValues.TryGetValue(columnName, out ColumnValue? columnValue))
            return columnValue;

        return null;
    }

    /// <summary>
    /// Step #1. Creates a new update plan for the table defining which unique indexes will be updated
    /// </summary>
    /// <param name="table"></param>
    /// <returns></returns>
    private static UpdateFluxIndexState GetIndexUpdatePlan(TableDescriptor table, UpdateTicket ticket)
    {
        UpdateFluxIndexState indexState = new();

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            if (index.Value.Type != IndexType.Unique)
                continue;

            if (index.Key != "~pk" && !ticket.Values.ContainsKey(index.Value.Column))
                continue;

            indexState.UniqueIndexes.Add(index.Value);
        }

        return indexState;
    }

    /// <summary>
    /// We need to locate the row tuples to Update
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> LocateTuplesToUpdate(UpdateFluxState state)
    {
        UpdateTicket ticket = state.Ticket;

        QueryTicket queryTicket = new(
            txnId: ticket.TxnId,
            databaseName: ticket.DatabaseName,
            tableName: ticket.TableName,
            index: null,
            filters: ticket.Filters,
            where: ticket.Where,
            orderBy: null,
            parameters: ticket.Parameters
        );

        state.DataCursor = state.QueryExecutor.Query(state.Database, state.Table, queryTicket);

        //Console.WriteLine("Data Pk={0} is at page offset {1}", ticket.Id, state.RowTuple.SlotTwo);*/

        return Task.FromResult(FluxAction.Continue);
    }

    private async Task UpdateMultiIndexes(DatabaseDescriptor database, TableDescriptor table, Dictionary<string, ColumnValue> columnValues)
    {
        BufferPoolHandler tablespace = database.TableSpace;

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes) // @todo update in parallel
        {
            if (index.Value.Type != IndexType.Multi)
                continue;

            if (index.Value.MultiRows is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A multi index tree wasn't found"
                );

            ColumnValue? columnValue = GetColumnValue(columnValues, index.Value.Column);
            if (columnValue is null) // @todo check what to to here
                continue;

            BTreeMulti<ColumnValue> multiIndex = index.Value.MultiRows;

            await indexSaver.Remove(tablespace, multiIndex, columnValue);
        }
    }

    /// <summary>
    /// Checks if a row with the same primary key is already added to table
    /// </summary>
    /// <param name="table"></param>
    /// <param name="uniqueIndex"></param>
    /// <param name="ticket"></param>
    /// <param name="name"></param>
    /// <returns></returns>
    /// <exception cref="CamusDBException"></exception>
    private static async Task CheckUniqueKeyViolations(
        TableDescriptor table,
        string keyName,
        BTree<ColumnValue, BTreeTuple?> uniqueIndex,
        HLCTimestamp txnId,
        Dictionary<string, ColumnValue> values,
        string name
    )
    {
        ColumnValue? uniqueValue = GetColumnValue(values, name);

        if (uniqueValue is null)
            return;

        BTreeTuple? rowTuple = await uniqueIndex.Get(txnId, uniqueValue);

        if (rowTuple is not null)
            throw new CamusDBException(
                CamusDBErrorCodes.DuplicateUniqueKeyValue,
                "Duplicate entry for key \"" + table.Name + "." + keyName + "\" " + uniqueValue.Type + " " + uniqueValue
            );
    }

    /// <summary>
    /// Check for unique key violations on every unique index the table has
    /// </summary>
    /// <param name="table"></param>
    /// <param name="txnId"></param>
    /// <param name="values"></param>
    /// <returns></returns>
    /// <exception cref="CamusDBException"></exception>
    private async Task CheckUniqueKeys(TableDescriptor table, HLCTimestamp txnId, Dictionary<string, ColumnValue> values)
    {
        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            if (index.Value.Type != IndexType.Unique)
                continue;

            BTree<ColumnValue, BTreeTuple?>? uniqueIndex = index.Value.UniqueRows;

            if (uniqueIndex is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A unique index tree wasn't found"
                );

            await CheckUniqueKeyViolations(table, index.Key, uniqueIndex, txnId, values, index.Value.Column);
        }
    }

    /// <summary>
    /// Updates the row on the disk
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> UpdateRowsAndIndexes(UpdateFluxState state)
    {
        if (state.DataCursor is null)
        {
            Console.WriteLine("Invalid rows to update");
            return FluxAction.Abort;
        }

        TableDescriptor table = state.Table;
        UpdateTicket ticket = state.Ticket;
        BufferPoolHandler tablespace = state.Database.TableSpace;
        BTreeMutationDeltas<ObjectIdValue, ObjectIdValue>? mainTableDeltas;
        List<(BTree<ColumnValue, BTreeTuple?>, BTreeMutationDeltas<ColumnValue, BTreeTuple?>)>? uniqueIndexDeltas;

        // @todo we need to take a snapshot of the data to prevent deadlocks
        // but probably need to optimize this for larger datasets
        List<QueryResultRow> rowsToUpdate = await state.DataCursor.ToListAsync();

        //Console.WriteLine("Unique indexes {0}", state.Indexes.UniqueIndexes.Count);

        foreach (QueryResultRow row in rowsToUpdate)
        {
            await CheckUniqueKeys(table, ticket.TxnId, ticket.Values);

            BTreeTuple tuple = UpdateNewRowVersionDisk(tablespace, table, state, row, ticket);

            mainTableDeltas = await UpdateTableIndex(state, tuple);

            uniqueIndexDeltas = await UpdateUniqueIndexes(state, ticket, tuple, row);

            await PersistIndexChanges(state, mainTableDeltas, uniqueIndexDeltas);

            Console.WriteLine(
                "Row with rowid {0} updated to page {1}",
                tuple.SlotOne,
                tuple.SlotTwo
            );

            state.ModifiedRows++;
        }

        return FluxAction.Continue;
    }

    private BTreeTuple UpdateNewRowVersionDisk(BufferPoolHandler tablespace, TableDescriptor table, UpdateFluxState state, QueryResultRow row, UpdateTicket ticket)
    {
        foreach (KeyValuePair<string, ColumnValue> keyValuePair in ticket.Values)        
            row.Row[keyValuePair.Key] = keyValuePair.Value;

        byte[] buffer = rowSerializer.Serialize(table, row.Row, row.Tuple.SlotOne);

        // Allocate a new page for the row
        BTreeTuple tuple = new(
            slotOne: row.Tuple.SlotOne,
            slotTwo: tablespace.GetNextFreeOffset()
        );

        tablespace.WriteDataToPageBatch(state.ModifiedPages, tuple.SlotTwo, 0, buffer);

        return tuple;
    }   

    private async Task<BTreeMutationDeltas<ObjectIdValue, ObjectIdValue>?> UpdateTableIndex(UpdateFluxState state, BTreeTuple tuple)
    {
        SaveUniqueOffsetIndexTicket saveUniqueOffsetIndex = new(
            index: state.Table.Rows,
            txnId: state.Ticket.TxnId,
            key: tuple.SlotOne,
            value: tuple.SlotTwo
        );

        // Main table index stores rowid pointing to page offset
        return await indexSaver.Save(saveUniqueOffsetIndex);
    }

    private async Task<List<(BTree<ColumnValue, BTreeTuple?>, BTreeMutationDeltas<ColumnValue, BTreeTuple?>)>> UpdateUniqueIndexes(UpdateFluxState state, UpdateTicket ticket, BTreeTuple tuple, QueryResultRow row)
    {        
        List<(BTree<ColumnValue, BTreeTuple?>, BTreeMutationDeltas<ColumnValue, BTreeTuple?>)> deltas = new();

        //Console.WriteLine("Updating unique indexes {0}", state.Indexes.UniqueIndexes.Count);

        foreach (TableIndexSchema index in state.Indexes.UniqueIndexes)
        {
            BTree<ColumnValue, BTreeTuple?>? uniqueIndex = index.UniqueRows;

            if (uniqueIndex is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A unique index tree wasn't found"
                );

            ColumnValue? uniqueKeyValue = GetColumnValue(row.Row, index.Column);

            if (uniqueKeyValue is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A null value was found for unique key field " + index.Column
                );

            SaveUniqueIndexTicket saveUniqueIndexTicket = new(
                index: uniqueIndex,
                txnId: ticket.TxnId,
                commitState: BTreeCommitState.Uncommitted,
                key: uniqueKeyValue,
                value: tuple
            );

            //Console.WriteLine("Saving unique index {0} {1} {2}", uniqueIndex, uniqueKeyValue, tuple);

            deltas.Add((uniqueIndex, await indexSaver.Save(saveUniqueIndexTicket)));
        }

        return deltas;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task PersistIndexChanges(UpdateFluxState state, BTreeMutationDeltas<ObjectIdValue, ObjectIdValue>? mainIndexDeltas, List<(BTree<ColumnValue, BTreeTuple?>, BTreeMutationDeltas<ColumnValue, BTreeTuple?>)> uniqueIndexDeltas)
    {
        if (mainIndexDeltas is null)
            return;

        foreach (BTreeMvccEntry<ObjectIdValue> btreeEntry in mainIndexDeltas.Entries)
            btreeEntry.CommitState = BTreeCommitState.Committed;

        await indexSaver.Persist(state.Database.TableSpace, state.Table.Rows, state.ModifiedPages, mainIndexDeltas);

        if (uniqueIndexDeltas is null)
            return;

        foreach ((BTree<ColumnValue, BTreeTuple?> index, BTreeMutationDeltas<ColumnValue, BTreeTuple?> deltas) uniqueIndex in uniqueIndexDeltas)
        {
            foreach (BTreeMvccEntry<BTreeTuple?> uniqueIndexEntry in uniqueIndex.deltas.Entries)
                uniqueIndexEntry.CommitState = BTreeCommitState.Committed;

            await indexSaver.Persist(state.Database.TableSpace, uniqueIndex.index, state.ModifiedPages, uniqueIndex.deltas);
        }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> ApplyPageOperations(UpdateFluxState state)
    {
        if (state.ModifiedPages.Count > 0)
            state.Database.TableSpace.ApplyPageOperations(state.ModifiedPages);

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// Executes the flux state machine to update records by the specified filters
    /// </summary>
    /// <param name="machine"></param>
    /// <param name="state"></param>
    /// <returns></returns>
    internal async Task<int> UpdateInternal(FluxMachine<UpdateFluxSteps, UpdateFluxState> machine, UpdateFluxState state)
    {
        DatabaseDescriptor database = state.Database;
        BufferPoolHandler tablespace = state.Database.TableSpace;
        TableDescriptor table = state.Table;
        UpdateTicket ticket = state.Ticket;

        Stopwatch timer = Stopwatch.StartNew();

        machine.When(UpdateFluxSteps.LocateTupleToUpdate, LocateTuplesToUpdate);
        machine.When(UpdateFluxSteps.UpdateRowsAndIndexes, UpdateRowsAndIndexes);
        machine.When(UpdateFluxSteps.ApplyPageOperations, ApplyPageOperations);

        //machine.WhenAbort(ReleaseLocks);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        timer.Stop();

        TimeSpan timeTaken = timer.Elapsed;

        Console.WriteLine(
            "Updated {0} rows, Time taken: {1}",
            state.ModifiedRows,
            timeTaken.ToString(@"m\:ss\.fff")
        );

        return state.ModifiedRows;
    }
}
