
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct UpdateByIdTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    public string Id { get; }

    public Dictionary<string, ColumnValue> Values { get; }

    public UpdateByIdTicket(string database, string name, string id, Dictionary<string, ColumnValue> columnValues)
    {
        DatabaseName = database;
        TableName = name;
        Id = id;
        Values = columnValues;
    }
}