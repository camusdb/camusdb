
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Journal.Models.Logs;

namespace CamusDB.Core.Journal.Models;

public sealed class JournalLogGroup
{
    public JournalGroupType Type { get; }

    public List<IJournalLog> Logs { get; } = new();

    public JournalLogGroup(JournalGroupType type)
    {
        Type = type;
    }
}
