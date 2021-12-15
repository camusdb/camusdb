
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Journal.Models;
using CamusDB.Core.CommandsExecutor;

namespace CamusDB.Core.Journal;

public sealed class JournalRecoverer
{
    public async Task Recover(CommandExecutor executor, Dictionary<uint, JournalLogGroup> logGroups)
    {
        foreach (KeyValuePair<uint, JournalLogGroup> logGroup in logGroups)
        {
            switch (logGroup.Value.Type)
            {
                case JournalGroupType.Insert:
                    //InsertRecoverer.
                    break;
            }
        }
    }
}
