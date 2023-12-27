
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct CreateDatabaseTicket
{
    public string DatabaseName { get; }

    public bool IfNotExists { get; }

    public CreateDatabaseTicket(string name, bool ifNotExists)
    {
        DatabaseName = name;
        IfNotExists = ifNotExists;
    }
}
