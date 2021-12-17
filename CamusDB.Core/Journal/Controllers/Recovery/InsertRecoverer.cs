
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Flux;
using CamusDB.Core.Journal.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.Journal.Models.Logs;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;

namespace CamusDB.Core.Journal.Controllers;

internal static class InsertRecoverer
{
    public static async Task Recover(CommandExecutor executor, DatabaseDescriptor database, JournalLogGroup group)
    {
        ///Console.WriteLine(group.Logs.Count);

        InsertLog? insertLog = GetInsertLog(group.Logs);
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
            ticket: new InsertTicket(database.Name, insertLog.TableName, insertLog.Values)
        );

        FluxMachine<InsertFluxSteps, InsertFluxState> machine = new(state);

        await executor.InsertWithState(machine, state);
    }

    private static InsertLog? GetInsertLog(List<IJournalLog> logs)
    {
        foreach (IJournalLog journalLog in logs)
        {
            if (journalLog is InsertLog log)
                return log;
        }

        return null;
    }
}
