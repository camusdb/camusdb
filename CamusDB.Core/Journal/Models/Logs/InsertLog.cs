
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Journal.Attributes;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Journal.Models.Logs;

[JournalSerializable(JournalLogTypes.Insert)]
public sealed class InsertLog : IJournalLog
{
    [JournalField(0)]
    public uint Sequence { get; }

    [JournalField(1)]
    public string TableName { get; }

    [JournalField(2)]
    public Dictionary<string, ColumnValue> Values { get; }

    public InsertLog(uint sequence, string name, Dictionary<string, ColumnValue> values)
    {
        Sequence = sequence;
        TableName = name;
        Values = values;
    }
}

