
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Journal.Controllers;

namespace CamusDB.Core.Journal;

public sealed class JournalManager
{
    public JournalWriter Writer { get; }

    public JournalManager(string databaseName)
    {
        Writer = new JournalWriter(databaseName);
    }
}
