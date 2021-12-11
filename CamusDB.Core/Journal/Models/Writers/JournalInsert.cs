﻿
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Journal.Attributes;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.Journal.Models.Writers;

[JournalSerializable]
public sealed class JournalInsert
{
    [JournalField(0)]
    public string TableName { get; }

    [JournalField(1)]
    public Dictionary<string, ColumnValue> Values { get; }

    public JournalInsert(string name, Dictionary<string, ColumnValue> values)
    {
        TableName = name;
        Values = values;
    }
}
