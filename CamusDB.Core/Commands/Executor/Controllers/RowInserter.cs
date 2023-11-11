
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Flux;
using System.Diagnostics;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Flux.Models;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;
using CamusDB.Core.CommandsExecutor.Controllers.Insert;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class RowInserter
{
    private readonly IndexSaver indexSaver = new();

    private readonly RowSerializer rowSerializer = new();

    private readonly InsertMultiKeySaver insertMultiKeySaver = new();

    private readonly InsertUniqueKeySaver insertUniqueKeySaver = new();

    /// <summary>
    /// Validates that all columns and values in the insert statement are valid
    /// </summary>
    /// <param name="table"></param>
    /// <param name="ticket"></param>
    /// <exception cref="CamusDBException"></exception>
    private static void Validate(TableDescriptor table, InsertTicket ticket) // @todo optimize this
    {
        List<TableColumnSchema> columns = table.Schema!.Columns!;

        foreach (KeyValuePair<string, ColumnValue> columnValue in ticket.Values)
        {
            bool hasColumn = false;

            for (int i = 0; i < columns.Count; i++)
            {
                TableColumnSchema column = columns[i];
                if (column.Name == columnValue.Key)
                {
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
    }

    /// <summary>
    /// Schedules a new insert operation
    /// </summary>
    /// <param name="database"></param>
    /// <param name="table"></param>
    /// <param name="ticket"></param>
    /// <returns></returns>
    public async Task Insert(DatabaseDescriptor database, TableDescriptor table, InsertTicket ticket)
    {
        Validate(table, ticket);

        InsertFluxState state = new(
            database: database,
            table: table,
            ticket: ticket,
            indexes: GetIndexInsertPlan(table)
        );

        FluxMachine<InsertFluxSteps, InsertFluxState> machine = new(state);

        await InsertInternal(machine, state);
    }

    /// <summary>
    /// Schedules a new insert operation by passing the flux state directly
    /// </summary>
    /// <param name="machine"></param>
    /// <param name="state"></param>
    /// <returns></returns>
    public async Task InsertWithState(FluxMachine<InsertFluxSteps, InsertFluxState> machine, InsertFluxState state)
    {
        await InsertInternal(machine, state);
    }

    /// <summary>
    /// Step #1. Creates a new insert plan for the table defining which unique indexes will be updated
    /// </summary>
    /// <param name="table"></param>
    /// <returns></returns>
    private static InsertFluxIndexState GetIndexInsertPlan(TableDescriptor table)
    {
        InsertFluxIndexState indexState = new();

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            if (index.Value.Type != IndexType.Unique)
                continue;

            indexState.UniqueIndexes.Add(index.Value);
        }

        return indexState;
    }

    /// <summary>
    /// Second step is check for unique key violations
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> CheckUniqueKeysStep(InsertFluxState state)
    {
        await insertUniqueKeySaver.CheckUniqueKeys(state.Table, state.Ticket);
        return FluxAction.Continue;
    }

    /// <summary>
    /// If there are no unique key violations we can allocate a tuple to insert the row
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> AllocateInsertTuple(InsertFluxState state)
    {
        BufferPoolHandler tablespace = state.Database.TableSpace;

        state.RowTuple.SlotOne = await tablespace.GetNextRowId();
        state.RowTuple.SlotTwo = await tablespace.GetNextFreeOffset();

        return FluxAction.Continue;
    }

    /**
     * Unique keys after updated before inserting the actual row
     */
    private async Task<FluxAction> UpdateUniqueKeysStep(InsertFluxState state)
    {
        UpdateUniqueIndexTicket ticket = new(
            database: state.Database,
            table: state.Table,
            sequence: state.Sequence,
            rowTuple: state.RowTuple,
            ticket: state.Ticket,
            indexes: state.Indexes.UniqueIndexes,
            locks: state.Locks,
            modifiedPages: state.ModifiedPages
        );

        await insertUniqueKeySaver.UpdateUniqueKeys(ticket);

        return FluxAction.Continue;
    }

    /// <summary>
    /// Allocate a new page in the buffer pool and insert the serializated row into it
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> InsertToPageStep(InsertFluxState state)
    {
        byte[] rowBuffer = rowSerializer.Serialize(state.Table, state.Ticket, state.RowTuple.SlotOne);

        // Insert data to the page offset
        state.ModifiedPages.Add(new InsertModifiedPage(state.RowTuple.SlotTwo, state.Sequence, rowBuffer));

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// Every table has a B+Tree index where the data can be easily located by rowid
    /// We take the page created in the previous step and insert it into the tree
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> UpdateTableIndex(InsertFluxState state)
    {
        BufferPoolHandler tablespace = state.Database.TableSpace;

        SaveUniqueOffsetIndexTicket saveUniqueOffsetIndex = new(
            tablespace: tablespace,
            index: state.Table.Rows,
            key: state.RowTuple.SlotOne,
            value: state.RowTuple.SlotTwo,
            locks: state.Locks,
            modifiedPages: state.ModifiedPages
        );

        //Console.WriteLine("{0}", state.Table.Rows.loaded);

        // Main table index stores rowid pointing to page offeset
        await indexSaver.Save(saveUniqueOffsetIndex);

        return FluxAction.Continue;
    }

    /// <summary>
    /// In the last step multi indexes are updated
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> UpdateMultiIndexes(InsertFluxState state)
    {
        SaveMultiKeysIndexTicket saveMultiKeysIndex = new(
            database: state.Database,
            table: state.Table,
            ticket: state.Ticket,
            rowTuple: state.RowTuple,
            locks: state.Locks,
            modifiedPages: state.ModifiedPages
        );

        await insertMultiKeySaver.UpdateMultiKeys(saveMultiKeysIndex);

        return FluxAction.Continue;
    }    

    /// <summary>
    /// All locks are released once the operation is successful
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> ReleaseLocks(InsertFluxState state)
    {
        foreach (IDisposable disposable in state.Locks)
            disposable.Dispose();

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// Creates a new flux machine and runs all steps in order
    /// </summary>
    /// <param name="machine"></param>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task InsertInternal(FluxMachine<InsertFluxSteps, InsertFluxState> machine, InsertFluxState state)
    {
        Stopwatch timer = Stopwatch.StartNew();

        machine.When(InsertFluxSteps.CheckUniqueKeys, CheckUniqueKeysStep);
        machine.When(InsertFluxSteps.UpdateUniqueKeys, UpdateUniqueKeysStep);
        machine.When(InsertFluxSteps.AllocateInsertTuple, AllocateInsertTuple);
        machine.When(InsertFluxSteps.InsertToPage, InsertToPageStep);
        machine.When(InsertFluxSteps.UpdateTableIndex, UpdateTableIndex);
        machine.When(InsertFluxSteps.UpdateMultiIndexes, UpdateMultiIndexes);
        machine.When(InsertFluxSteps.ReleaseLocks, ReleaseLocks);

        machine.WhenAbort(ReleaseLocks);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        await state.Database.TableSpace.WriteDataToPages(state.ModifiedPages);

        TimeSpan timeTaken = timer.Elapsed;

        Console.WriteLine(
            "Row {0} inserted at {1}, Time taken: {2}",
            state.RowTuple.SlotOne,
            state.RowTuple.SlotTwo,
            timeTaken.ToString(@"m\:ss\.fff")
        );
    }
}

/*foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
{
    if (index.Value.MultiRows is not null)
    {
        foreach (BTreeMultiEntry entry in index.Value.MultiRows.EntriesTraverse())
        {
            Console.WriteLine("Index Key={0}/{1} PageOffset={2}", index.Key, entry.Key, entry.Value!.Size());

            foreach (BTreeEntry entry2 in entry.Value.EntriesTraverse())
            {
                Console.WriteLine(" > Index Key={0} PageOffset={1}", entry2.Key, entry2.Value);
            }
        }
    }
}*/