
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Flux;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Journal.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Journal.Models.Logs;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;
using CamusDB.Core.BufferPool;

namespace CamusDB.Core.Journal.Controllers;

internal static class InsertRecoverer
{
    /*
     * In order to recover the partial insert we need to know which steps are in the journal
     * so we can perform the remaining ones
     */
    public static async Task Recover(CommandExecutor executor, DatabaseDescriptor database, JournalLogGroup group)
    {
        // Get main insert log from group
        InsertLog? insertLog = GetLog<InsertLog>(group.Logs);
        if (insertLog is null)
            throw new Exception("Couldn't load insert log from insert group");

        OpenTableTicket ticket = new(
            database: database.Name,
            name: insertLog.TableName
        );

        TableDescriptor table = await executor.OpenTableWithDescriptor(database, ticket);

        InsertFluxState state = new(
            database: database,
            table: table,
            ticket: new InsertTicket(database.Name, insertLog.TableName, insertLog.Values),
            indexes: GetIndexInsertPlan(table, group.Logs)
        );

        InsertFluxSteps step = await GetRestoreState(database, state, group, insertLog.Sequence);

        Console.WriteLine("Partial insert {0} will be restored at {1}", insertLog.Sequence, step);

        FluxMachine<InsertFluxSteps, InsertFluxState> machine = new(
            state,
            step
        );

        await executor.InsertWithState(machine, state);
    }

    private static async Task<InsertFluxSteps> GetRestoreState(
        DatabaseDescriptor database,
        InsertFluxState state,
        JournalLogGroup group,
        uint originalSequence)
    {
        BufferPoolHandler tablespace = database.TableSpace;

        // Check if group has insert slots log
        InsertSlotsLog? insertSlotsLog = GetLog<InsertSlotsLog>(group.Logs);
        if (insertSlotsLog is null)
            return InsertFluxSteps.AllocateInsertTuple;

        // Assign the row tuple found before update any indexes
        state.RowTuple = insertSlotsLog.RowTuple;

        // Check If there are remaining unique indexes to update
        if (state.Indexes.UniqueIndexes.Count > 0)
            return InsertFluxSteps.UpdateUniqueKeys;

        // Check if the row was inserted at the specified page
        if (state.RowTuple.SlotOne > -1)
        {
            uint flushedSequence = await tablespace.GetSequenceFromPage(state.RowTuple.SlotTwo);
            Console.WriteLine("FlushedSequence={0} ", flushedSequence, originalSequence);

            if (flushedSequence != originalSequence)
                return InsertFluxSteps.InsertToPage;
        }

        // Check if table index has been updated
        UpdateTableIndexLog? updateTableIndexLog = GetLog<UpdateTableIndexLog>(group.Logs);
        if (updateTableIndexLog is null)
            return InsertFluxSteps.UpdateTableIndex;

        UpdateTableIndexCheckpointLog? updateTableIndexCheckpointLog = GetLog<UpdateTableIndexCheckpointLog>(group.Logs);
        if (updateTableIndexCheckpointLog is null)
            return InsertFluxSteps.UpdateTableIndex;

        // if indexes are already updated then just checkpoint the insert
        return InsertFluxSteps.CheckpointInsert;
    }

    /*
     * Checks if updates to unique indexes were completed, return the list of remaining indexes
     */
    private static InsertFluxIndexState GetIndexInsertPlan(TableDescriptor table, List<IJournalLog> logs)
    {
        InsertFluxIndexState indexState = new();

        Dictionary<string, bool> updatedIndexes = new();

        foreach (IJournalLog journalLog in logs)
        {
            if (journalLog is UpdateUniqueIndexLog log)
            {
                updatedIndexes.Add(log.ColumnIndex, false);
                continue;
            }

            if (journalLog is UpdateUniqueCheckpointLog checkpointLog)
            {
                //Console.WriteLine("{0}?", checkpointLog.ColumnIndex);
                updatedIndexes[checkpointLog.ColumnIndex] = true;
                continue;
            }
        }

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            if (index.Value.Type != IndexType.Unique)
                continue;

            if (updatedIndexes.TryGetValue(index.Value.Column, out bool completed))
            {
                //Console.WriteLine("{0} {1}", index.Value.Column, completed);

                if (!completed)
                    indexState.UniqueIndexes.Add(index.Value);
            }
            else
            {
                indexState.UniqueIndexes.Add(index.Value);
            }
        }

        //Console.WriteLine(indexState.UniqueIndexes.Count);

        return indexState;
    }

    private static T? GetLog<T>(List<IJournalLog> logs)
    {
        foreach (IJournalLog journalLog in logs)
        {
            if (journalLog is T log)
                return log;
        }

        return default(T);
    }
}
