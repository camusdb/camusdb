
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct ExecuteSQLTicket
{
    public string DatabaseName { get; }

    public string Sql { get; }

    public Dictionary<string, ColumnValue>? Parameters { get; }

    public ExecuteSQLTicket(string database, string sql, Dictionary<string, ColumnValue>? parameters)
    {
        DatabaseName = database;
        Sql = sql;
        Parameters = parameters;
    }
}
