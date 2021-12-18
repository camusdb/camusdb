
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Flux;
using System.Diagnostics;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Flux.Models;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Journal.Models.Logs;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;
using CamusDB.Core.CommandsExecutor.Controllers.Insert;
using CamusDB.Core.Journal.Controllers;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class RowInserter
{
    private readonly IndexSaver indexSaver = new();

    private readonly RowSerializer rowSerializer = new();

    private readonly InsertMultiKeySaver insertMultiKeySaver = new();

    private readonly InsertUniqueKeySaver insertUniqueKeySaver = new();

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
                    "Unknown column '" + columnValue.Key + "' in column list"
                );
        }
    }

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

    /**
     * First step is insert in the WAL
     */
    private async Task<FluxAction> InitializeStep(InsertFluxState state)
    {
        InsertLog schedule = new(0, state.Ticket.TableName, state.Ticket.Values);
        state.Sequence = await state.Database.Journal.Writer.Append(state.Ticket.ForceFailureType, schedule);
        return FluxAction.Continue;
    }

    /**
     * Second step is check for unique key violations
     */
    private FluxAction CheckUniqueKeysStep(InsertFluxState state)
    {
        insertUniqueKeySaver.CheckUniqueKeys(state.Table, state.Ticket);
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
            indexes: state.Indexes.UniqueIndexes
        );

        state.RowTuple = await insertUniqueKeySaver.UpdateUniqueKeys(ticket);
        return FluxAction.Continue;
    }

    /**
     * Allocate a new page in the buffer pool and insert the serializated row into it
     */
    private async Task<FluxAction> InsertToPageStep(InsertFluxState state)
    {
        BufferPoolHandler tablespace = state.Database.TableSpace;

        byte[] rowBuffer = rowSerializer.Serialize(state.Table, state.Ticket, state.RowTuple.SlotOne);

        WritePageLog writeSchedule = new(state.Sequence, 0, rowBuffer);
        await state.Database.Journal.Writer.Append(state.Ticket.ForceFailureType, writeSchedule);

        // Insert data to the page offset
        await tablespace.WriteDataToPage(state.RowTuple.SlotTwo, state.Sequence, rowBuffer);

        return FluxAction.Continue;
    }

    /**
     * Every table has a B+Tree index where the data can be easily located by rowid
     * We take the page created in the previous step and insert it in the tree
     */
    private async Task<FluxAction> UpdateTableIndex(InsertFluxState state)
    {
        BufferPoolHandler tablespace = state.Database.TableSpace;
        JournalWriter journalWriter = state.Database.Journal.Writer;

        UpdateTableIndexLog indexSchedule = new(state.Sequence);
        uint updateIndexSequence = await journalWriter.Append(state.Ticket.ForceFailureType, indexSchedule);

        // Main table index stores rowid pointing to page offeset
        await indexSaver.Save(tablespace, state.Table.Rows, state.RowTuple.SlotOne, state.RowTuple.SlotTwo);

        UpdateTableIndexCheckpointLog checkpointSchedule = new(state.Sequence, updateIndexSequence);
        await journalWriter.Append(state.Ticket.ForceFailureType, checkpointSchedule);

        return FluxAction.Continue;
    }

    /**
     * In the last step multi indexes are updated
     */
    private async Task<FluxAction> UpdateMultiIndexes(InsertFluxState state)
    {
        await insertMultiKeySaver.UpdateMultiKeys(state.Database, state.Table, state.Ticket, state.RowTuple);
        return FluxAction.Continue;
    }

    /**
     * Finally we checkpoint the insert to the WAL
     */
    private async Task<FluxAction> CheckpointInsert(InsertFluxState state)
    {
        InsertCheckpointLog insertCheckpoint = new(state.Sequence);
        await state.Database.Journal.Writer.Append(state.Ticket.ForceFailureType, insertCheckpoint);
        return FluxAction.Completed;
    }   

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

    public async Task InsertWithState(FluxMachine<InsertFluxSteps, InsertFluxState> machine, InsertFluxState state)
    {
        await InsertInternal(machine, state);
    }

    private async Task InsertInternal(FluxMachine<InsertFluxSteps, InsertFluxState> machine, InsertFluxState state)
    {
        Stopwatch timer = new();
        timer.Start();

        machine.When(InsertFluxSteps.NotInitialized, InitializeStep);
        machine.When(InsertFluxSteps.CheckUniqueKeys, CheckUniqueKeysStep);
        machine.When(InsertFluxSteps.UpdateUniqueKeys, UpdateUniqueKeysStep);
        machine.When(InsertFluxSteps.InsertToPage, InsertToPageStep);
        machine.When(InsertFluxSteps.UpdateTableIndex, UpdateTableIndex);
        machine.When(InsertFluxSteps.UpdateMultiIndexes, UpdateMultiIndexes);
        machine.When(InsertFluxSteps.CheckpointInsert, CheckpointInsert);

        machine.WhenAbort(CheckpointInsert);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        timer.Stop();

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