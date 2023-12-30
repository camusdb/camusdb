
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct DropTableTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }    

    public DropTableTicket(string database, string name)
    {
        DatabaseName = database;
        TableName = name;        
    }
}

