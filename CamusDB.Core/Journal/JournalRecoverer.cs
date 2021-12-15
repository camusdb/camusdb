
using CamusDB.Core.Journal.Models;

namespace CamusDB.Core.Journal;

public class JournalRecoverer
{
    public JournalRecoverer()
    {
        foreach (KeyValuePair<uint, JournalLogGroup> group in logGroups)
        {
            switch (group.Value.Type)
            {
                case JournalGroupType.Insert:
                    //InsertRecoverer.
                    break;

                default:
                    throw new Exception("Unknown group type " + group.Value.Type);
            }

            Console.WriteLine("Incomplete insert found {0}");
        }
    }
}
