
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Serializer;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Journal.Models;
using CamusDB.Core.Serializer.Models;
using CamusDB.Core.Journal.Attributes;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Journal.Models.Logs;

[JournalSerializable(JournalLogTypes.Insert)]
public sealed class InsertLog
{
    [JournalField(0)]
    public string TableName { get; }

    [JournalField(1)]
    public Dictionary<string, ColumnValue> Values { get; }

    public InsertLog(string name, Dictionary<string, ColumnValue> values)
    {
        TableName = name;
        Values = values;
    }
}

