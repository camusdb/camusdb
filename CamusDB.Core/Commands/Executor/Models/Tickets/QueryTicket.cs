
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct QueryTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    public string? IndexName { get; }

    public List<QueryFilter>? Filters { get; }

    public QueryTicket(string database, string name, string? index, List<QueryFilter>? filters)
    {
        DatabaseName = database;
        TableName = name;
        IndexName = index;
        Filters = filters;
    }
}
