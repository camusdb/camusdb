
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct QueryTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    public string? IndexName { get; }

    public List<QueryFilter>? Filters { get; }

    public NodeAst? Where { get; }

    public List<QueryOrderBy>? OrderBy { get; }

    public QueryTicket(string database, string name, string? index, List<QueryFilter>? filters, NodeAst? where, List<QueryOrderBy>? orderBy)
    {
        DatabaseName = database;
        TableName = name;
        IndexName = index;
        Filters = filters;
        Where = where;
        OrderBy = orderBy;
    }
}
