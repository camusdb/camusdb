
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Journal.Models;
using CamusDB.Core.CommandsExecutor;

namespace CamusDB.Core.Journal;

public sealed class JournalVerifier
{
    public async Task<Dictionary<uint, JournalLogGroup>> Verify(string path)
    {
        JournalReader journalReader = new(path);

        Dictionary<uint, JournalLogGroup> logGroups = new();

        //Console.WriteLine("Started verification");

        await foreach (JournalLog journalLog in journalReader.ReadNextLog())
        {
            Console.WriteLine("{0} {1}", journalLog.Sequence, journalLog.Type);

            switch (journalLog.Type)
            {
                case JournalLogTypes.Insert:
                    logGroups.Add(journalLog.Sequence, new JournalLogGroup());
                    break;

                case JournalLogTypes.InsertCheckpoint:
                    uint parentSequence = journalLog.InsertCheckpointLog!.Sequence;

                    if (!logGroups.TryGetValue(parentSequence, out JournalLogGroup? group))
                    {
                        Console.WriteLine("Insert checkpoint {0} not found", parentSequence);
                        break;
                    }

                    Console.WriteLine("Removed insert {0} from journal", parentSequence);

                    logGroups.Remove(parentSequence); // Insert is complete so remove

                    break;
            }
        }
        
        journalReader.Dispose();

        return logGroups;                     
    }
}
