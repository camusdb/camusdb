
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Journal.Models;

namespace CamusDB.Core.Journal.Controllers;

public sealed class JournalVerifier
{
    public async Task<Dictionary<uint, JournalLogGroup>> Verify(string path)
    {
        JournalReader journalReader = new(path);

        Dictionary<uint, JournalLogGroup> logGroups = new();

        //Console.WriteLine("Started verification");

        await foreach (JournalLog journalLog in journalReader.ReadNextLog())
        {
            uint parentSequence;
            JournalLogGroup? group;

            //Console.WriteLine("{0} {1}", journalLog.Sequence, journalLog.Type);

            switch (journalLog.Type)
            {
                case JournalLogTypes.Insert:
                    logGroups.Add(journalLog.Sequence, new JournalLogGroup(JournalGroupType.Insert));
                    break;

                case JournalLogTypes.WritePage:
                case JournalLogTypes.InsertSlots:
                case JournalLogTypes.UpdateUniqueIndex:
                case JournalLogTypes.UpdateUniqueIndexCheckpoint:
                    parentSequence = journalLog.Log!.Sequence;
                    if (logGroups.TryGetValue(parentSequence, out group))
                        group.Logs.Add(journalLog.Log);
                    else
                        Console.WriteLine("Group {0}/{1} not found", parentSequence, journalLog.Type);
                    break;

                case JournalLogTypes.InsertCheckpoint:
                    parentSequence = journalLog.Log!.Sequence;
                    if (!logGroups.TryGetValue(parentSequence, out group))
                    {
                        Console.WriteLine("Insert checkpoint {0} not found", parentSequence);
                        break;
                    }

                    Console.WriteLine("Removed insert {0} from journal", parentSequence);
                    logGroups.Remove(parentSequence); // Insert is complete so remove
                    break;

                default:
                    Console.WriteLine("Unknown {0}", journalLog.Type);
                    break;
            }
        }

        journalReader.Dispose();

        return logGroups;
    }
}
