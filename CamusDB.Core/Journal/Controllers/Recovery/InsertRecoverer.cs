
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.Journal.Models;

namespace CamusDB.Core.Journal.Controllers;

internal static class InsertRecoverer
{
    public static async Task Recover(CommandExecutor executor, JournalLogGroup group)
    {
        Console.WriteLine(group.Logs.Count);
    }
}
