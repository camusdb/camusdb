
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct DeleteTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    public NodeAst? Where { get; }

    public List<QueryFilter>? Filters { get; }

    public DeleteTicket(
        string database,
        string name,
        NodeAst? where,
        List<QueryFilter>? filters
    )
    {
        DatabaseName = database;
        TableName = name;
        Where = where;
        Filters = filters;
    }
}
