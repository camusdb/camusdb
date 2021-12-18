
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

namespace CamusDB.Core.Journal.Controllers;

internal static class InsertRecoverer
{
    /*
     * In order to recover the partial insert we need to know which steps are in the journal
     * so we can perform the remaining ones
     */
    public static async Task Recover(CommandExecutor executor, DatabaseDescriptor database, JournalLogGroup group)
    {
        InsertFluxSteps step = InsertFluxSteps.NotInitialized;

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

        // If there are no unique indexes to update insert data to page
        if (state.Indexes.UniqueIndexes.Count > 0)
            step = InsertFluxSteps.UpdateUniqueKeys;

        // Check if group has insert slots log
        InsertSlotsLog? insertSlotsLog = GetLog<InsertSlotsLog>(group.Logs);
        if (insertSlotsLog is not null)
            state.RowTuple = insertSlotsLog.RowTuple;

        // if indexes are already updated then just insert page data
        if (state.Indexes.UniqueIndexes.Count == 0 && state.RowTuple.SlotOne > -1)
            step = InsertFluxSteps.InsertToPage;
        
        UpdateTableIndexLog? updateTableIndexLog = GetLog<UpdateTableIndexLog>(group.Logs);
        UpdateTableIndexCheckpointLog? updateTableIndexCheckpointLog = GetLog<UpdateTableIndexCheckpointLog>(group.Logs);

        // Check if table index has been updated
        if (updateTableIndexLog is null || (updateTableIndexLog is not null && updateTableIndexCheckpointLog is null))
            step = InsertFluxSteps.UpdateTableIndex;

        Console.WriteLine(
            "Recovering at step {0} RowId={1} PageNum={2} Indexes={3}",
            step,
            state.RowTuple.SlotOne,
            state.RowTuple.SlotTwo,
            state.Indexes.UniqueIndexes.Count
        );

        FluxMachine<InsertFluxSteps, InsertFluxState> machine = new(state, step);

        await executor.InsertWithState(machine, state);
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
                break;
            }

            if (journalLog is UpdateUniqueCheckpointLog checkpointLog)
            {
                updatedIndexes[checkpointLog.ColumnIndex] = true;
                break;
            }
        }

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            if (index.Value.Type != IndexType.Unique)
                continue;

            if (updatedIndexes.TryGetValue(index.Value.Column, out bool completed))
            {
                if (!completed)
                    indexState.UniqueIndexes.Add(index.Value);
            }
            else
            {
                indexState.UniqueIndexes.Add(index.Value);
            }
        }

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
