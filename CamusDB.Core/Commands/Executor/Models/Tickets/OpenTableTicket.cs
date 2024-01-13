
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct OpenTableTicket
{
	public string DatabaseName { get; }

	public string TableName { get; }

	public OpenTableTicket(string databaseName, string tableName)
	{
		DatabaseName = databaseName;
		TableName = tableName;
	}
}
