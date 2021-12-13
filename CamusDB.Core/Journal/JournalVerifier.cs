
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Journal.Models;

namespace CamusDB.Core.Journal;

public sealed class JournalVerifier
{
    public JournalVerifier()
    {
    }

    public async Task Verify(string path)
    {
        JournalReader journalReader = new(path);

        Dictionary<uint, JournalLogGroup> logGroups = new();

        await foreach (JournalLog journalLog in journalReader.ReadNextLog())
        {
            switch (journalLog.Type)
            {
                case JournalLogTypes.Insert:
                    logGroups.Add(journalLog.Sequence, new JournalLogGroup());
                    break;

                case JournalLogTypes.InsertCheckpoint:
                    uint parentSequence = journalLog.InsertCheckpointLog!.Sequence;

                    if (!logGroups.TryGetValue(parentSequence, out JournalLogGroup? group))
                        break;

                    Console.WriteLine("Removed insert {0} from journal", parentSequence);

                    logGroups.Remove(parentSequence); // Insert is complete so remove

                    break;
            }
        }

        journalReader.Dispose();
    }
}
