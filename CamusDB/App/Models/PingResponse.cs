
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

public sealed class PingResponse
{
	public string Status { get; }

    public DateTime DateTime { get; }

    public PingResponse(string status, DateTime dateTime)
	{
		Status = status;
		DateTime = dateTime;
	}
}
