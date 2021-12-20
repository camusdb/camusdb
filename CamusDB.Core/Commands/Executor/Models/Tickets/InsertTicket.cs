
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Journal.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public sealed class InsertTicket
{
    public string DatabaseName { get; }
    
    public string TableName { get; }

    public Dictionary<string, ColumnValue> Values { get; }

    public JournalFailureTypes ForceFailureType { get; }

    public InsertTicket(
        string database,
        string name,
        Dictionary<string, ColumnValue> values,
        JournalFailureTypes forceFailureType = JournalFailureTypes.None)
    {
        DatabaseName = database;
        TableName = name;
        Values = values;
        ForceFailureType = forceFailureType;
    }
}

