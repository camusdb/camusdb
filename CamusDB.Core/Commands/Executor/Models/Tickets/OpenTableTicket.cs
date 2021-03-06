
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public sealed class OpenTableTicket
{
	public string DatabaseName { get; }

	public string TableName { get; }

	public OpenTableTicket(string database, string name)
	{
		DatabaseName = database;
		TableName = name;
	}
}
