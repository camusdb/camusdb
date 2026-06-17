
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct RenameDatabaseTicket
{
    public string OldName { get; }

    public string NewName { get; }

    public RenameDatabaseTicket(string oldName, string newName)
    {
        OldName = oldName;
        NewName = newName;
    }
}
