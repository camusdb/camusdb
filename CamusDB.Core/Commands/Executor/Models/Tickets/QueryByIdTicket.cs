
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public sealed class QueryByIdTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    public string Id { get; }

    public QueryByIdTicket(string database, string name, string id)
    {
        DatabaseName = database;
        TableName = name;
        Id = id;
    }
}
