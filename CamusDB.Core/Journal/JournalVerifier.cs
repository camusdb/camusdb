
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

        await foreach (JournalLog journalLog in journalReader.ReadNextLog())
        {
            Console.WriteLine(journalLog.Type);
        }

        journalReader.Dispose();
    }
}
