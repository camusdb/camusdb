
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public class DeleteByIdTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    public int Id { get; }

    public DeleteByIdTicket(string database, string name, int id)
    {
        DatabaseName = database;
        TableName = name;
        Id = id;
    }
}
