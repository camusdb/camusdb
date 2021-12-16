
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Journal.Models.Logs;

namespace CamusDB.Core.Journal.Models;

public sealed class JournalLog
{
    public uint Sequence { get; }

    public JournalLogTypes Type { get; }

    public IJournalLog? Log { get; }

    public JournalLog(uint sequence, JournalLogTypes type, IJournalLog log)
    {
        Sequence = sequence;
        Type = type;
        Log = log;
    }    
}
