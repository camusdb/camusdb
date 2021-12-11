
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
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Journal.Models.Readers;

public sealed class InsertTicketLog
{ 
    public string TableName { get; }

    public Dictionary<string, ColumnValue> Values { get; }

    public InsertTicketLog(string name, Dictionary<string, ColumnValue> values)
    {
        TableName = name;
        Values = values;
    }
}

