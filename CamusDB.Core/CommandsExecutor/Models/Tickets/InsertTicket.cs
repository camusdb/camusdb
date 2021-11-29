
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public sealed class InsertTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }    

    public Dictionary<string, ColumnValue> Values { get; }

    public InsertTicket(string database, string name, Dictionary<string, ColumnValue> values)
    {
        DatabaseName = database;
        TableName = name;        
        Values = values;
    }
}

