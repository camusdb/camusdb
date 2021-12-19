
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Journal.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.Journal.Controllers;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Journal.Controllers;

public sealed class JournalRecoverer
{
    public async Task<List<JournalRecoverResult>> Recover(CommandExecutor executor, DatabaseDescriptor database, Dictionary<uint, JournalLogGroup> logGroups)
    {
        List<JournalRecoverResult> results = new();

        foreach (KeyValuePair<uint, JournalLogGroup> logGroup in logGroups)
        {
            switch (logGroup.Value.Type)
            {
                case JournalGroupType.Insert:
                    results.Add(await InsertRecoverer.Recover(executor, database, logGroup.Key, logGroup.Value));
                    break;

                default:
                    throw new Exception("Unknown recovery group type: " + logGroup.Value.Type);
            }
        }

        return results;
    }
}
