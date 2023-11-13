
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct UpdateTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    public Dictionary<string, ColumnValue> Values { get; }

    public UpdateTicket(
        string database,
        string name,
        Dictionary<string, ColumnValue> values)
    {
        DatabaseName = database;
        TableName = name;
        Values = values;
    }
}
